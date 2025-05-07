#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import calendar
import collections
import copy
import functools
import inspect
import itertools
import json
import logging
import re
from collections import Counter
import typing
import uuid
from collections.abc import Hashable, Iterable, Mapping, Sequence
from datetime import timedelta, tzinfo
from functools import reduce
from typing import Any, Callable, List, Literal, Optional, TypeVar, Union, get_args

import modin.pandas as pd
import numpy as np
import numpy.typing as npt
import pandas as native_pd
import pandas.core.resample
import pandas.io.parsers
from pandas.core.interchange.dataframe_protocol import DataFrame as InterchangeDataframe
import pandas.io.parsers.readers
import pytz  # type: ignore
from modin.core.storage_formats import BaseQueryCompiler  # type: ignore
from pandas import Timedelta
from pandas._libs import lib
from pandas._libs.lib import no_default
from pandas._libs.tslibs import Tick
from pandas._libs.tslibs.offsets import BusinessDay, CustomBusinessDay, Day
from pandas._typing import (
    AggFuncType,
    AnyArrayLike,
    Axes,
    Axis,
    DateTimeErrorChoices,
    DtypeBackend,
    FillnaOptions,
    Frequency,
    IgnoreRaise,
    IndexKeyFunc,
    IndexLabel,
    Level,
    NaPosition,
    RandomState,
    Renamer,
    Scalar,
    SortKind,
    Suffixes,
)
from pandas.api.types import (
    is_bool,
    is_bool_dtype,
    is_datetime64_any_dtype,
    is_integer_dtype,
    is_named_tuple,
    is_numeric_dtype,
    is_re_compilable,
    is_scalar,
    is_string_dtype,
    is_timedelta64_dtype,
)
from pandas.core.dtypes.base import ExtensionDtype
from pandas.core.dtypes.common import is_dict_like, is_list_like, pandas_dtype
from pandas.core.indexes.base import ensure_index
from pandas.errors import DataError
from pandas.io.formats.format import format_percentiles
from pandas.io.formats.printing import PrettyDict

from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    quote_name_without_upper_casing,
)
from snowflake.snowpark._internal.type_utils import ColumnOrName
from snowflake.snowpark._internal.utils import (
    generate_random_alphanumeric,
    parse_table_name,
    random_name_for_temp_object,
)
from snowflake.snowpark.column import CaseExpr, Column as SnowparkColumn
from snowflake.snowpark.dataframe import DataFrame as SnowparkDataFrame
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import (
    abs as abs_,
    array_construct,
    array_size,
    array_slice,
    bround,
    builtin,
    cast,
    coalesce,
    col,
    concat,
    corr,
    count,
    count_distinct,
    date_from_parts,
    date_part,
    dateadd,
    dayofmonth,
    dayofyear,
    dense_rank,
    first_value,
    floor,
    get,
    greatest,
    hour,
    iff,
    initcap,
    is_char,
    is_null,
    lag,
    last_value,
    lead,
    least,
    length,
    lower,
    lpad,
    ltrim,
    max as max_,
    min as min_,
    minute,
    month,
    negate,
    not_,
    object_keys,
    pandas_udf,
    quarter,
    random,
    rank,
    regexp_replace,
    reverse,
    round as snowpark_round,
    row_number,
    rpad,
    rtrim,
    second,
    substring,
    sum as sum_,
    sum_distinct,
    timestamp_ntz_from_parts,
    to_date,
    to_time,
    to_variant,
    translate,
    trim,
    trunc,
    uniform,
    upper,
    when,
    year,
)
from snowflake.snowpark.modin.plugin._internal import (
    concat_utils,
    generator_utils,
    join_utils,
    get_dummies_utils,
)
from snowflake.snowpark.modin.plugin._internal.aggregation_utils import (
    AGG_NAME_COL_LABEL,
    AggFuncInfo,
    AggFuncWithLabel,
    AggregateColumnOpParameters,
    _columns_coalescing_idxmax_idxmin_helper,
    aggregate_with_ordered_dataframe,
    check_is_aggregation_supported_in_snowflake,
    column_quantile,
    convert_agg_func_arg_to_col_agg_func_map,
    drop_non_numeric_data_columns,
    generate_column_agg_info,
    get_agg_func_to_col_map,
    get_pandas_aggr_func_name,
    get_snowflake_agg_func,
    is_first_last_in_agg_funcs,
    repr_aggregate_function,
    using_named_aggregations_for_func,
)
from snowflake.snowpark.modin.plugin._internal.align_utils import (
    align_axis_0_left,
    align_axis_0_right,
    align_axis_1,
)
from snowflake.snowpark.modin.plugin._internal.apply_utils import (
    ALL_SNOWFLAKE_CORTEX_FUNCTIONS,
    APPLY_LABEL_COLUMN_QUOTED_IDENTIFIER,
    APPLY_VALUE_COLUMN_QUOTED_IDENTIFIER,
    DEFAULT_UDTF_PARTITION_SIZE,
    GroupbyApplySortMethod,
    SUPPORTED_SNOWFLAKE_CORTEX_FUNCTIONS_IN_APPLY,
    check_return_variant_and_get_return_type,
    create_udf_for_series_apply,
    create_udtf_for_apply_axis_1,
    create_udtf_for_groupby_apply,
    create_internal_frame_for_groupby_apply_no_pivot_result,
    deduce_return_type_from_function,
    get_metadata_from_groupby_apply_pivot_result_column_names,
    groupby_apply_create_internal_frame_from_final_ordered_dataframe,
    groupby_apply_pivot_result_to_final_ordered_dataframe,
    groupby_apply_sort_method,
    is_supported_snowpark_python_function,
    make_series_map_snowpark_function,
    sort_apply_udtf_result_columns_by_pandas_positions,
)
from collections import defaultdict
from snowflake.snowpark.modin.plugin._internal.binary_op_utils import (
    BinaryOp,
    merge_label_and_identifier_pairs,
    prepare_binop_pairs_between_dataframe_and_dataframe,
)
from snowflake.snowpark.modin.plugin._internal.cumulative_utils import (
    get_cumagg_col_to_expr_map_axis0,
    get_groupby_cumagg_frame_axis0,
)
from snowflake.snowpark.modin.plugin._internal.cut_utils import (
    compute_bin_indices,
    preprocess_bins_for_cut,
)
from snowflake.snowpark.modin.plugin._internal.frame import (
    InternalFrame,
    LabelIdentifierPair,
)
from snowflake.snowpark.modin.plugin._internal.groupby_utils import (
    check_is_groupby_supported_by_snowflake,
    resample_and_extract_groupby_column_pandas_labels,
    get_frame_with_groupby_columns_as_index,
    get_groups_for_ordered_dataframe,
    make_groupby_rank_col_for_method,
    validate_groupby_columns,
    extract_groupby_column_pandas_labels,
    fill_missing_groupby_resample_bins_for_frame,
    validate_groupby_resample_supported_by_snowflake,
)
from snowflake.snowpark.modin.plugin._internal.indexing_utils import (
    ValidIndex,
    convert_snowpark_row_to_pandas_index,
    get_frame_by_col_label,
    get_frame_by_col_pos,
    get_frame_by_row_label,
    get_frame_by_row_pos_frame,
    get_frame_by_row_pos_slice_frame,
    get_index_frame_by_row_label_slice,
    get_row_pos_frame_from_row_key,
    get_snowflake_filter_for_row_label,
    get_valid_col_pos_list_from_columns,
    get_valid_index_values,
    set_frame_2d_labels,
    set_frame_2d_positional,
)
from snowflake.snowpark.modin.plugin._internal.io_utils import (
    TO_CSV_DEFAULTS,
    get_columns_to_keep_for_usecols,
    get_compression_algorithm_for_csv,
    get_non_pandas_kwargs,
    is_local_filepath,
    upload_local_path_to_snowflake_stage,
)
from snowflake.snowpark.modin.plugin._internal.isin_utils import (
    compute_isin_with_dataframe,
    compute_isin_with_series,
    convert_values_to_list_of_literals_and_return_type,
    scalar_isin_expression,
)
from snowflake.snowpark.modin.plugin._internal.join_utils import (
    InheritJoinIndex,
    JoinKeyCoalesceConfig,
    MatchComparator,
    convert_index_type_to_variant,
)
from snowflake.snowpark.modin.plugin._internal.ordered_dataframe import (
    DataFrameReference,
    OrderedDataFrame,
    OrderingColumn,
)
from snowflake.snowpark.modin.plugin._internal.pivot_utils import (
    expand_pivot_result_with_pivot_table_margins,
    expand_pivot_result_with_pivot_table_margins_no_groupby_columns,
    generate_pivot_aggregation_value_label_snowflake_quoted_identifier_mappings,
    generate_single_pivot_labels,
    pivot_helper,
)
from snowflake.snowpark.modin.plugin._internal.resample_utils import (
    IMPLEMENTED_AGG_METHODS,
    RULE_SECOND_TO_DAY,
    fill_missing_resample_bins_for_frame,
    get_expected_resample_bins_frame,
    get_snowflake_quoted_identifier_for_resample_index_col,
    perform_asof_join_on_frame,
    perform_resample_binning_on_frame,
    rule_to_snowflake_width_and_slice_unit,
    validate_resample_supported_by_snowflake,
    compute_resample_start_and_end_date,
)
from snowflake.snowpark.modin.plugin._internal.snowpark_pandas_types import (
    SnowparkPandasColumn,
    SnowparkPandasType,
    TimedeltaType,
)
from snowflake.snowpark.modin.plugin._internal.timestamp_utils import (
    VALID_TO_DATETIME_DF_KEYS,
    DateTimeOrigin,
    col_to_timedelta,
    generate_timestamp_col,
    raise_if_to_datetime_not_supported,
    timedelta_freq_to_nanos,
    to_snowflake_timestamp_format,
    tz_convert_column,
    tz_localize_column,
)
from snowflake.snowpark.modin.plugin._internal.transpose_utils import (
    clean_up_transpose_result_index_and_labels,
    prepare_and_unpivot_for_transpose,
    transpose_empty_df,
)
from snowflake.snowpark.modin.plugin._internal.type_utils import (
    DataTypeGetter,
    TypeMapper,
    column_astype,
    infer_object_type,
    is_astype_type_error,
    is_compatible_snowpark_types,
)
from snowflake.snowpark.modin.plugin._internal.unpivot_utils import (
    StackOperation,
    unpivot,
    unpivot_empty_df,
)
from snowflake.snowpark.modin.plugin._internal.utils import (
    INDEX_LABEL,
    ROW_COUNT_COLUMN_LABEL,
    ROW_POSITION_COLUMN_LABEL,
    SAMPLED_ROW_POSITION_COLUMN_LABEL,
    FillNAMethod,
    TempObjectType,
    append_columns,
    cache_result,
    check_snowpark_pandas_object_in_arg,
    check_valid_pandas_labels,
    count_rows,
    create_frame_with_data_columns,
    create_ordered_dataframe_from_pandas,
    create_initial_ordered_dataframe,
    extract_all_duplicates,
    extract_pandas_label_from_snowflake_quoted_identifier,
    fill_missing_levels_for_pandas_label,
    fill_none_in_index_labels,
    fillna_label_to_value_map,
    generate_snowflake_quoted_identifiers_helper,
    get_default_snowpark_pandas_statement_params,
    get_distinct_rows,
    get_mapping_from_left_to_right_columns_by_label,
    infer_snowpark_types_from_pandas,
    is_all_label_components_none,
    is_duplicate_free,
    label_prefix_match,
    pandas_lit,
    parse_object_construct_snowflake_quoted_identifier_and_extract_pandas_label,
    parse_snowflake_object_construct_identifier_to_map,
    unquote_name_if_quoted,
)
from snowflake.snowpark.modin.plugin._internal.where_utils import (
    validate_expected_boolean_data_columns,
)
from snowflake.snowpark.modin.plugin._internal.window_utils import (
    WindowFunction,
    check_and_raise_error_expanding_window_supported_by_snowflake,
    check_and_raise_error_rolling_window_supported_by_snowflake,
    create_snowpark_interval_from_window,
    get_rolling_corr_column,
)
from snowflake.snowpark.modin.plugin._typing import (
    DropKeep,
    JoinTypeLit,
    ListLike,
    PandasLabelToSnowflakeIdentifierPair,
    SnowflakeSupportedFileTypeLit,
)
from snowflake.snowpark.modin.plugin.utils.error_message import ErrorMessage
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage
from snowflake.snowpark.modin.utils import MODIN_UNNAMED_SERIES_LABEL
from snowflake.snowpark.modin.plugin.utils.numpy_to_pandas import (
    NUMPY_UNIVERSAL_FUNCTION_TO_SNOWFLAKE_FUNCTION,
)
from snowflake.snowpark.row import Row
from snowflake.snowpark.session import Session
from snowflake.snowpark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    MapType,
    PandasDataFrameType,
    PandasSeriesType,
    StringType,
    TimestampTimeZone,
    TimestampType,
    TimeType,
    VariantType,
    _IntegralType,
    _NumericType,
)
from snowflake.snowpark.udf import UserDefinedFunction
from snowflake.snowpark.window import Window

_logger = logging.getLogger(__name__)

# TODO: SNOW-1229442 remove this restriction once bug in quantile is fixed.
# For now, limit number of quantiles supported df.quantiles to avoid producing recursion limit failure in Snowpark.
MAX_QUANTILES_SUPPORTED: int = 16

_GROUPBY_UNSUPPORTED_GROUPING_MESSAGE = "does not yet support axis == 1, by != None and level != None, or by containing any non-pandas hashable labels."

QUARTER_START_MONTHS = [1, 4, 7, 10]

SUPPORTED_DT_FLOOR_CEIL_FREQS = ["day", "hour", "minute", "second"]

SECONDS_PER_DAY = 86400
NANOSECONDS_PER_SECOND = 10**9
NANOSECONDS_PER_MICROSECOND = 10**3
MICROSECONDS_PER_SECOND = 10**6
NANOSECONDS_PER_DAY = SECONDS_PER_DAY * NANOSECONDS_PER_SECOND

# Matches pandas
_TIMEDELTA_ROLLING_AGGREGATION_NOT_SUPPORTED = "No numeric types to aggregate"
# Matches pandas
_TIMEDELTA_ROLLING_CORR_NOT_SUPPORTED = (
    "ops for Rolling for this dtype timedelta64[ns] are not implemented"
)

# List of query compiler methods where attrs on the result should always be empty.
_RESET_ATTRS_METHODS = [
    "compare",
    "merge",
    "value_counts",
    "dataframe_to_datetime",
    "series_to_datetime",
    "to_numeric",
    "dt_isocalendar",
    "groupby_all",
    "groupby_any",
    "groupby_cumcount",
    "groupby_cummax",
    "groupby_cummin",
    "groupby_cumsum",
    "groupby_nunique",
    "groupby_rank",
    "groupby_size",
    "groupby_pct_change",
    # expanding and rolling methods also do not propagate; we check them by prefix matching
    # agg, crosstab, and concat depend on their inputs, and are handled separately
]


T = TypeVar("T", bound=Callable[..., Any])


def _propagate_attrs_on_methods(cls):  # type: ignore
    """
    Decorator that modifies all methods on the class to copy `_attrs` from `self`
    to the output of the method, if the output is another query compiler.
    """

    def propagate_attrs_decorator(method: T) -> T:
        @functools.wraps(method)
        def wrap(self, *args, **kwargs):  # type: ignore
            result = method(self, *args, **kwargs)
            if isinstance(result, SnowflakeQueryCompiler) and len(self._attrs):
                result._attrs = copy.deepcopy(self._attrs)
            return result

        return typing.cast(T, wrap)

    def reset_attrs_decorator(method: T) -> T:
        @functools.wraps(method)
        def wrap(self, *args, **kwargs):  # type: ignore
            result = method(self, *args, **kwargs)
            if isinstance(result, SnowflakeQueryCompiler) and len(self._attrs):
                result._attrs = {}
            return result

        return typing.cast(T, wrap)

    for attr_name, attr_value in cls.__dict__.items():
        # concat is handled explicitly because it checks all of its arguments
        # agg is handled explicitly because it sometimes resets and sometimes propagates
        if attr_name.startswith("_") or attr_name in ["concat", "agg"]:
            continue
        if attr_name in _RESET_ATTRS_METHODS or any(
            attr_name.startswith(prefix) for prefix in ["expanding", "rolling"]
        ):
            setattr(cls, attr_name, reset_attrs_decorator(attr_value))
        elif isinstance(attr_value, property):
            setattr(
                cls,
                attr_name,
                property(
                    propagate_attrs_decorator(
                        attr_value.fget
                        if attr_value.fget is not None
                        else attr_value.__get__
                    ),
                    propagate_attrs_decorator(
                        attr_value.fset
                        if attr_value.fset is not None
                        else attr_value.__set__
                    ),
                    propagate_attrs_decorator(
                        attr_value.fdel
                        if attr_value.fdel is not None
                        else attr_value.__delete__
                    ),
                ),
            )
        elif inspect.isfunction(attr_value):
            setattr(cls, attr_name, propagate_attrs_decorator(attr_value))
    return cls


@_propagate_attrs_on_methods
class SnowflakeQueryCompiler(BaseQueryCompiler):
    """based on: https://modin.readthedocs.io/en/0.11.0/flow/modin/backends/base/query_compiler.html
    this class is best explained by looking at https://github.com/modin-project/modin/blob/a8be482e644519f2823668210cec5cf1564deb7e/modin/experimental/core/storage_formats/hdk/query_compiler.py
    """

    # When these laziness flags are set, upstream Modin elides some length checks that would incur queries.
    lazy_row_labels = True
    lazy_row_count = True
    lazy_column_types = False
    lazy_column_labels = False
    lazy_column_count = False

    def __init__(self, frame: InternalFrame) -> None:
        """this stores internally a local pandas object (refactor this)"""
        assert frame is not None and isinstance(
            frame, InternalFrame
        ), "frame is None or not a InternalFrame"
        self._modin_frame = frame
        # self.snowpark_pandas_api_calls a list of lazy Snowpark pandas telemetry api calls
        # Copying and modifying self.snowpark_pandas_api_calls and self._method_call_counts
        # is taken care of in telemetry decorators
        self.snowpark_pandas_api_calls: list = []
        self._attrs: dict[Any, Any] = {}
        self._method_call_counts: Counter[str] = Counter[str]()

    def _raise_not_implemented_error_for_timedelta(
        self, frame: InternalFrame = None
    ) -> None:
        """Raise NotImplementedError for SnowflakeQueryCompiler methods which does not support timedelta yet."""
        if frame is None:
            frame = self._modin_frame
        for val in frame.snowflake_quoted_identifier_to_snowpark_pandas_type.values():
            if isinstance(val, TimedeltaType):
                method = inspect.currentframe().f_back.f_back.f_code.co_name  # type: ignore[union-attr]
                ErrorMessage.not_implemented_for_timedelta(method)

    def _warn_lost_snowpark_pandas_type(self) -> None:
        """Warn Snowpark pandas type can be lost in current operation."""
        method = inspect.currentframe().f_back.f_back.f_code.co_name  # type: ignore[union-attr]
        snowpark_pandas_types = [
            type(t).__name__
            for t in set(
                self._modin_frame.cached_data_column_snowpark_pandas_types
                + self._modin_frame.cached_index_column_snowpark_pandas_types
            )
            if t is not None
        ]
        if snowpark_pandas_types:
            WarningMessage.lost_type_warning(
                method,
                ", ".join(snowpark_pandas_types),
            )

    def snowpark_pandas_type_immutable_check(func: Callable) -> Any:
        """The decorator to check on SnowflakeQueryCompiler methods which return a new SnowflakeQueryCompiler.
        It verifies the cached Snowpark pandas types should not be changed.
        """

        def check_type(input: List, output: List) -> None:
            assert len(input) == len(
                output
            ), "self frame and output frame have different number of columns"

            for lt, rt in zip(input, output):
                assert (
                    lt == rt
                ), f"one column's Snowpark pandas type has been changed from {lt} to {rt}"

        @functools.wraps(func)
        def wrap(*args, **kwargs):  # type: ignore
            self_qc = args[0]
            output_qc = func(*args, **kwargs)
            assert isinstance(self_qc, SnowflakeQueryCompiler) and isinstance(
                output_qc, SnowflakeQueryCompiler
            ), (
                "immutable_snowpark_pandas_type_check only works with SnowflakeQueryCompiler member methods with "
                "SnowflakeQueryCompiler as the return result"
            )
            check_type(
                self_qc._modin_frame.cached_index_column_snowpark_pandas_types,
                output_qc._modin_frame.cached_index_column_snowpark_pandas_types,
            )
            check_type(
                self_qc._modin_frame.cached_data_column_snowpark_pandas_types,
                output_qc._modin_frame.cached_data_column_snowpark_pandas_types,
            )

            return output_qc

        return wrap

    def _get_dtypes(
        self, snowflake_quoted_identifiers: List[str]
    ) -> List[Union[np.dtype, ExtensionDtype]]:
        """
        Get dtypes for the input columns.

        Args:
            snowflake_quoted_identifiers: input column identifiers

        Returns:
            a list of the dtypes.
        """
        type_map = self._modin_frame.quoted_identifier_to_snowflake_type(
            snowflake_quoted_identifiers
        )
        return [
            self._modin_frame.get_datetime64tz_from_timestamp_tz(i)
            if t == TimestampType(TimestampTimeZone.TZ)
            else self._modin_frame.get_datetime64tz_from_timestamp_ltz()
            if t == TimestampType(TimestampTimeZone.LTZ)
            else TypeMapper.to_pandas(t)
            for i, t in type_map.items()
        ]

    @property
    def dtypes(self) -> native_pd.Series:
        """
        Get columns dtypes.

        Returns
        -------
        pandas.Series
            Series with dtypes of each column.
        """
        return native_pd.Series(
            data=self._get_dtypes(
                self._modin_frame.data_column_snowflake_quoted_identifiers
            ),
            index=self._modin_frame.data_columns_index,
            dtype=object,
        )

    @property
    def index_dtypes(self) -> list[Union[np.dtype, ExtensionDtype]]:
        """
        Get index dtypes.

        Returns
        -------
        pandas.Series
            Series with dtypes of each column.
        """
        return self._get_dtypes(
            self._modin_frame.index_column_snowflake_quoted_identifiers
        )

    def is_timestamp_type(self, idx: int, is_index: bool = True) -> bool:
        """Return True if column at the index is TIMESTAMP TYPE.

        Args:
            idx: the index of the column
            is_index: whether it is an index or data column
        """
        return isinstance(
            self._modin_frame.get_snowflake_type(
                self._modin_frame.index_column_snowflake_quoted_identifiers
                if is_index
                else self._modin_frame.data_column_snowflake_quoted_identifiers
            )[idx],
            TimestampType,
        )

    def is_datetime64_any_dtype(self, idx: int, is_index: bool = True) -> bool:
        """Helper method similar to is_datetime64_any_dtype, but it avoids extra query for DatetimeTZDtype.

        Args:
            idx: the index of the column
            is_index: whether it is an index or data column
        """
        return self.is_timestamp_type(idx, is_index)

    def is_timedelta64_dtype(self, idx: int, is_index: bool = True) -> bool:
        """Helper method similar to is_timedelta_dtype, but it avoids extra query for DatetimeTZDtype.

        Args:
            idx: the index of the column
            is_index: whether it is an index or data column
        """
        id = (
            self._modin_frame.index_column_snowflake_quoted_identifiers[idx]
            if is_index
            else self._modin_frame.data_column_snowflake_quoted_identifiers[idx]
        )
        return self._modin_frame.get_snowflake_type(id) == TimedeltaType()

    def is_string_dtype(self, idx: int, is_index: bool = True) -> bool:
        """Helper method similar to is_timedelta_dtype, but it avoids extra query for DatetimeTZDtype.

        Args:
            idx: the index of the column
            is_index: whether it is an index or data column
        """
        return not self.is_timestamp_type(idx, is_index) and is_string_dtype(
            self.index_dtypes[idx] if is_index else self.dtypes[idx]
        )

    @classmethod
    def from_pandas(
        cls, df: native_pd.DataFrame, *args: Any, **kwargs: Any
    ) -> "SnowflakeQueryCompiler":
        # create copy of original dataframe
        df = df.copy()
        # encode column labels to snowflake compliant strings.
        # If df.columns is a MultiIndex, it will become a list of tuples
        original_column_labels = df.columns.tolist()
        # if name is not set, df.columns.names will return FrozenList[None].
        original_column_index_names = df.columns.names

        # session.create_dataframe creates a temporary snowflake table from given pandas dataframe. Snowflake
        # tables do not support duplicate column names hence column names of pandas dataframe here must be de-duplicated
        # before passing this dataframe to create_dataframe() method. We de-duplicate pandas dataframe column names in
        # following two steps:
        # 1. Generate snowflake quoted identifiers which are duplicate free.
        # 2. Extract pandas labels from generated snowflake quoted identifiers and update columns of original dataframe.
        # Note: In our internal frame mapping we will continue to use original pandas labels (which may have duplicates)
        data_column_snowflake_quoted_identifiers = (
            generate_snowflake_quoted_identifiers_helper(
                pandas_labels=original_column_labels, excluded=[]
            )
        )
        # Extract pandas labels from snowflake quoted identifiers and reassign these new labels to pandas dataframe
        # before writing to temporary table.
        df.columns = [
            extract_pandas_label_from_snowflake_quoted_identifier(identifier)
            for identifier in data_column_snowflake_quoted_identifiers
        ]
        # Generate snowflake quoted identifier for index columns
        original_index_pandas_labels = df.index.names
        index_snowflake_quoted_identifiers = (
            generate_snowflake_quoted_identifiers_helper(
                pandas_labels=fill_none_in_index_labels(original_index_pandas_labels),
                excluded=data_column_snowflake_quoted_identifiers,
                wrap_double_underscore=True,
            )
        )
        current_df_data_column_snowflake_quoted_identifiers = (
            index_snowflake_quoted_identifiers
            + data_column_snowflake_quoted_identifiers
        )

        # reset index so the index can be a data column in the native pandas df
        # this is because write_pandas in python connector will not write the
        # index column into Snowflake
        # See https://github.com/snowflakedb/snowflake-connector-python/blob/main/src/snowflake/connector/pandas_tools.py
        df.reset_index(
            inplace=True,
            names=[
                extract_pandas_label_from_snowflake_quoted_identifier(identifier)
                for identifier in index_snowflake_quoted_identifiers
            ],
        )
        # need to keep row_position column (or expression in the future)
        # i.e., when https://snowflakecomputing.atlassian.net/browse/SNOW-767687 is done,
        # replace column with expression
        row_position_snowflake_quoted_identifier = (
            generate_snowflake_quoted_identifiers_helper(
                pandas_labels=[ROW_POSITION_COLUMN_LABEL],
                excluded=current_df_data_column_snowflake_quoted_identifiers,
                wrap_double_underscore=True,
            )[0]
        )

        df[
            extract_pandas_label_from_snowflake_quoted_identifier(
                row_position_snowflake_quoted_identifier
            )
        ] = np.arange(len(df))

        current_df_data_column_snowflake_quoted_identifiers.append(
            row_position_snowflake_quoted_identifier
        )

        # create snowpark df
        snowpark_pandas_types, snowpark_types = infer_snowpark_types_from_pandas(df)
        ordered_dataframe = create_ordered_dataframe_from_pandas(
            df,
            snowflake_quoted_identifiers=current_df_data_column_snowflake_quoted_identifiers,
            snowpark_types=snowpark_types,
            ordering_columns=[
                OrderingColumn(row_position_snowflake_quoted_identifier),
            ],
            row_position_snowflake_quoted_identifier=row_position_snowflake_quoted_identifier,
        )

        # construct the internal frame for the dataframe
        return cls(
            InternalFrame.create(
                ordered_dataframe=ordered_dataframe,
                data_column_pandas_labels=original_column_labels,
                data_column_pandas_index_names=original_column_index_names,
                # data columns appear after the index columns, but before the
                # row position column.
                data_column_types=snowpark_pandas_types[
                    len(index_snowflake_quoted_identifiers) : (
                        len(index_snowflake_quoted_identifiers)
                        + len(data_column_snowflake_quoted_identifiers)
                    )
                ],
                data_column_snowflake_quoted_identifiers=data_column_snowflake_quoted_identifiers,
                index_column_pandas_labels=original_index_pandas_labels,
                index_column_snowflake_quoted_identifiers=index_snowflake_quoted_identifiers,
                # The columns up to position `len(index_snowflake_quoted_identifiers)`
                # are the index columns.
                index_column_types=snowpark_pandas_types[
                    : len(index_snowflake_quoted_identifiers)
                ],
            )
        )

    @classmethod
    def from_arrow(cls, at: Any, *args: Any, **kwargs: Any) -> "SnowflakeQueryCompiler":
        return cls(at.to_pandas())

    def to_dataframe(
        self, nan_as_null: bool = False, allow_copy: bool = True
    ) -> InterchangeDataframe:
        return self.to_pandas().__dataframe__(
            nan_as_null=nan_as_null, allow_copy=allow_copy
        )

    @classmethod
    def from_dataframe(cls, df: native_pd.DataFrame, data_cls: Any) -> None:
        pass

    @classmethod
    def from_date_range(
        cls,
        start: Optional[pd.Timestamp],
        end: Optional[pd.Timestamp],
        periods: Optional[int],
        freq: Optional[pd.DateOffset],
        tz: Union[str, tzinfo],
        left_inclusive: bool,
        right_inclusive: bool,
    ) -> "SnowflakeQueryCompiler":
        """
        Snowpark pandas implementation for generating date ranges.

        Args:
            start : Timestamp, optional
                Left bound for generating dates.
            end : Timestamp, optional
                Right bound for generating dates.
            periods : int
                Number of periods to generate.
            freq : str or DateOffset
                Frequency strings can have multiples, e.g. '5H'. See
                :ref:`here <timeseries.offset_aliases>` for a list of
                frequency aliases.
            tz : str or tzinfo
                Time zone name for returning localized DatetimeIndex, for example
                'Asia/Hong_Kong'. By default, the resulting DatetimeIndex is
                timezone-naive.
            left_inclusive : bool
                Whether to include left boundary.
            right_inclusive : bool
                Whether to include right boundary.
        Returns:
            A series with generated datetime values in the target range
        """
        assert freq is not None or not any(
            x is None for x in [periods, start, end]
        ), "Must provide freq argument if no data is supplied"

        remove_non_business_days = False

        if freq is not None:
            if isinstance(freq, CustomBusinessDay):
                ErrorMessage.not_implemented("CustomBusinessDay is not supported.")
            if isinstance(freq, BusinessDay):
                freq = Day()
                remove_non_business_days = True
            # We break Day arithmetic (fixed 24 hour) here and opt for
            # Day to mean calendar day (23/24/25 hour). Therefore, strip
            # tz info from start and day to avoid DST arithmetic
            if isinstance(freq, Day):
                if start is not None:
                    start = start.tz_localize(None)
                if end is not None:
                    end = end.tz_localize(None)
            if isinstance(freq, Tick):
                # generate nanosecond values
                ns_values = generator_utils.generate_regular_range(
                    start, end, periods, freq
                )
                dt_values = ns_values.series_to_datetime()
            else:
                dt_values = generator_utils.generate_irregular_range(
                    start, end, periods, freq
                )
        else:
            # Create a linearly spaced date_range in local time
            # This is the original pandas source code:
            # i8values = (
            #   np.linspace(0, end.value - start.value, periods, dtype="int64")
            #   + start.value
            # )
            # Here we implement it similarly as np.linspace
            div = periods - 1  # type: ignore[operator]
            delta = end.value * 1.0 - start.value  # type: ignore[union-attr]
            if div == 0:
                # Only 1 period, just return the start value
                ns_values = pd.Series([start.value])._query_compiler  # type: ignore[union-attr]
            else:
                stride = delta / div
                # Make sure end is included in this case
                e = start.value + delta // stride * stride + stride // 2 + 1  # type: ignore[union-attr]
                ns_values = generator_utils.generate_range(start.value, e, stride)  # type: ignore[union-attr]
            dt_values = ns_values.series_to_datetime()

        dt_series = pd.Series(query_compiler=dt_values)
        if remove_non_business_days:
            dt_series = dt_series[dt_series.dt.dayofweek < 5]
        if not left_inclusive or not right_inclusive:
            if not left_inclusive and start is not None:
                dt_series = dt_series[dt_series != start].reset_index(drop=True)
            if not right_inclusive and end is not None:
                # No need to reset_index since we only removed the tail
                dt_series = dt_series[dt_series != end]
        return dt_series._query_compiler

    @snowpark_pandas_type_immutable_check
    def copy(self) -> "SnowflakeQueryCompiler":
        """
        Make a copy of this object.

        Returns:
            An instance of Snowflake query compiler.
        """
        # InternalFrame is immutable, it's safe to use same underlying instance for
        # multiple query compilers.
        qc = SnowflakeQueryCompiler(self._modin_frame)
        qc.snowpark_pandas_api_calls = self.snowpark_pandas_api_calls.copy()
        return qc

    def to_pandas(
        self,
        *,
        statement_params: Optional[dict[str, str]] = None,
        **kwargs: Any,
    ) -> native_pd.DataFrame:
        """
        Convert underlying query compilers data to ``pandas.DataFrame``.

        Args:
            statement_params: Dictionary of statement level parameters to be set while executing this action.

        Returns:
        pandas.DataFrame
            The QueryCompiler converted to pandas.

        """
        result = self._modin_frame.to_pandas(statement_params, **kwargs)
        if self._attrs:
            result.attrs = self._attrs
        return result

    def finalize(self) -> None:
        pass

    def free(self) -> None:
        pass

    def execute(self) -> None:
        pass

    def to_numpy(
        self,
        dtype: Optional[npt.DTypeLike] = None,
        copy: Optional[bool] = False,
        na_value: object = lib.no_default,
        **kwargs: Any,
    ) -> np.ndarray:
        # the modin version which has been forked here already supports an experimental numpy backend.
        # i.e., for something like df.values internally to_numpy().flatten() is called
        # with flatten being another query compiler call into the numpy frontend layer.
        # here it's overwritten to actually perform numpy conversion, i.e. return an actual numpy object
        if copy:
            WarningMessage.ignored_argument(
                operation="to_numpy",
                argument="copy",
                message="copy is ignored in Snowflake backend",
            )
        return self.to_pandas().to_numpy(dtype=dtype, na_value=na_value, **kwargs)

    def repartition(self, axis: Any = None) -> "SnowflakeQueryCompiler":
        # let Snowflake handle partitioning, it makes no sense to repartition the dataframe.
        return self

    def default_to_pandas(self, pandas_op: Callable, *args: Any, **kwargs: Any) -> None:
        func_name = pandas_op.__name__

        # this is coming from Modin's encoding scheme in default.py:build_default_to_pandas
        # encoded as f"<function {cls.OBJECT_TYPE}.{fn_name}>"
        # extract DataFrame operation, following extraction fails if not adhering to above format
        object_type, fn_name = func_name[len("<function ") : -1].split(".")

        # Previously, Snowpark pandas would register a stored procedure that materializes the frame
        # and performs the native pandas operation. Because this fallback has extremely poor
        # performance, we now raise NotImplementedError instead.
        ErrorMessage.not_implemented(
            f"Snowpark pandas doesn't yet support the method {object_type}.{fn_name} with the given arguments."
        )

    @classmethod
    def from_snowflake(
        cls,
        name_or_query: Union[str, Iterable[str]],
        index_col: Optional[Union[str, list[str]]] = None,
        columns: Optional[list[str]] = None,
        enforce_ordering: bool = False,
    ) -> "SnowflakeQueryCompiler":
        """
        See detailed docstring and examples in ``read_snowflake`` in frontend layer:
        src/snowflake/snowpark/modin/plugin/pd_extensions.py
        """
        if columns is not None and not isinstance(columns, list):
            raise ValueError("columns must be provided as list, i.e ['A'].")

        # create ordered dataframe, possibly with all columns in a read only table first
        (
            ordered_dataframe,
            row_position_snowflake_quoted_identifier,
        ) = create_initial_ordered_dataframe(
            table_name_or_query=name_or_query,
            enforce_ordering=enforce_ordering,
        )
        pandas_labels_to_snowflake_quoted_identifiers_map = {
            # pandas labels of resulting Snowpark pandas dataframe will be snowflake identifier
            # after stripping quotes. row_position is not included
            extract_pandas_label_from_snowflake_quoted_identifier(
                identifier
            ): identifier
            for identifier in ordered_dataframe.projected_column_snowflake_quoted_identifiers
            if identifier != row_position_snowflake_quoted_identifier
        }

        def find_snowflake_quoted_identifier(pandas_columns: list[str]) -> list[str]:
            """
            Returns the corresponding snowflake_quoted_identifier of column represented by
            a Python string if its value match the pandas label extracted from
            snowflake_quoted_identifier.
            """
            result = []
            for column in pandas_columns:
                if column not in pandas_labels_to_snowflake_quoted_identifiers_map:
                    raise KeyError(
                        f"{column} is not in existing snowflake columns {list(pandas_labels_to_snowflake_quoted_identifiers_map.values())}"
                    )
                result.append(pandas_labels_to_snowflake_quoted_identifiers_map[column])
            return result

        # find index columns from snowflake table
        # if not specified, index_column_snowflake_quoted_identifiers will be
        # row_position_snowflake_quoted_identifier and its label will be None,
        # which will be set at the end of this method.
        index_column_pandas_labels = []
        index_column_snowflake_quoted_identifiers = []
        if index_col:
            if isinstance(index_col, str):
                index_col = [index_col]
            index_column_pandas_labels = index_col
            index_column_snowflake_quoted_identifiers = (
                find_snowflake_quoted_identifier(index_col)
            )

        # find data columns from snowflake table
        if columns:
            data_column_pandas_labels = columns
            data_column_snowflake_quoted_identifiers = find_snowflake_quoted_identifier(
                data_column_pandas_labels
            )
        else:
            # if not specified, data_column_pandas_labels will be
            # all columns in the snowflake table except index columns and row position column
            data_column_pandas_labels = []
            data_column_snowflake_quoted_identifiers = []
            for (
                label,
                identifier,
            ) in pandas_labels_to_snowflake_quoted_identifiers_map.items():
                if identifier not in index_column_snowflake_quoted_identifiers:
                    data_column_pandas_labels.append(label)
                    data_column_snowflake_quoted_identifiers.append(identifier)

        # when there are duplicates in snowflake identifiers, we need to deduplicate
        snowflake_quoted_identifiers_to_be_selected = (
            index_column_snowflake_quoted_identifiers
            + data_column_snowflake_quoted_identifiers
        )
        if len(snowflake_quoted_identifiers_to_be_selected) != len(
            set(snowflake_quoted_identifiers_to_be_selected)
        ):
            pandas_labels_to_be_selected = (
                index_column_pandas_labels + data_column_pandas_labels
            )
            snowflake_quoted_identifiers_to_be_renamed = (
                generate_snowflake_quoted_identifiers_helper(
                    pandas_labels=pandas_labels_to_be_selected,
                    excluded=[row_position_snowflake_quoted_identifier],
                )
            )

            # get all columns we want to select with renaming duplicate columns in snowpark df
            ordered_dataframe = ordered_dataframe.select(
                [row_position_snowflake_quoted_identifier]
                + [
                    old_identifier
                    if old_identifier == new_identifier
                    else col(old_identifier).as_(new_identifier)
                    for old_identifier, new_identifier in zip(
                        snowflake_quoted_identifiers_to_be_selected,
                        snowflake_quoted_identifiers_to_be_renamed,
                    )
                ]
            )

            # get the index column and data column snowflake identifiers again
            # after deduplication and renaming
            num_index_columns = len(index_column_snowflake_quoted_identifiers)
            index_column_snowflake_quoted_identifiers = (
                snowflake_quoted_identifiers_to_be_renamed[:num_index_columns]
            )
            data_column_snowflake_quoted_identifiers = (
                snowflake_quoted_identifiers_to_be_renamed[num_index_columns:]
            )

        # set index column to row position column when index_col is not specified
        if not index_col:
            index_column_pandas_labels = [None]  # type: ignore[list-item]
            index_column_snowflake_quoted_identifiers = [
                row_position_snowflake_quoted_identifier
            ]

        return cls(
            InternalFrame.create(
                ordered_dataframe=ordered_dataframe,
                data_column_pandas_labels=data_column_pandas_labels,
                data_column_pandas_index_names=[
                    None
                ],  # no index names from snowflake table
                data_column_snowflake_quoted_identifiers=data_column_snowflake_quoted_identifiers,
                index_column_pandas_labels=index_column_pandas_labels,
                index_column_snowflake_quoted_identifiers=index_column_snowflake_quoted_identifiers,
                data_column_types=None,  # from_snowflake won't provide any client side type info
                index_column_types=None,
            )
        )

    @classmethod
    def from_file_with_pandas(
        cls,
        filetype: SnowflakeSupportedFileTypeLit,
        **kwargs: Any,
    ) -> "SnowflakeQueryCompiler":
        """
        Returns a SnowflakeQueryCompiler whose internal frame holds the data read from
        a file or multiple files.

        This method *only* handles local files, parsed using the native pandas parser.
        """
        # Arguments which must be handled as part of a post-processing stage
        local_exclude_set = ["index_col", "usecols"]
        local_kwargs = {k: v for k, v in kwargs.items() if k not in local_exclude_set}

        def is_names_set(kwargs: Any) -> bool:
            return kwargs["names"] is not no_default and kwargs["names"] is not None

        # For the purposes of the initial import we need to make sure the column names
        # are strings. These will be overriden later in post-processing.
        if is_names_set(local_kwargs):
            local_names = [str(n) for n in kwargs["names"]]
            local_kwargs["names"] = local_names

        # We explicitly do not support chunksize yet
        if local_kwargs["chunksize"] is not None:
            ErrorMessage.not_implemented("chunksize parameter not supported for files")
        # We could return an empty dataframe here, but it does not seem worth it.
        if is_list_like(kwargs["usecols"]) and len(kwargs["usecols"]) == 0:
            ErrorMessage.not_implemented(
                "empty 'usecols' parameter not supported for files"
            )

        # local file that begins with '@' (represents SF stage normally)
        if local_kwargs["filepath_or_buffer"].startswith(r"\@"):
            local_kwargs["filepath_or_buffer"] = local_kwargs["filepath_or_buffer"][1:]

        if filetype == "csv":
            df = native_pd.read_csv(**local_kwargs)
            # When names is shorter than the total number of columns an index
            # is created, regardless of the value of index_col. If this happens
            # we reset the index so the full dataset is uploaded to snowflake.
            if not isinstance(df.index, pandas.core.indexes.range.RangeIndex):
                df = df.reset_index()

            # Integer columns are not writable to snowflake; so we need to save
            # these names to fix the header during post processing
            if not is_names_set(kwargs) and kwargs["header"] is None:
                kwargs["names"] = list(df.columns.values)
                df.columns = df.columns.astype(str)

        temporary_table_name = random_name_for_temp_object(TempObjectType.TABLE)
        pd.session.write_pandas(
            df=df,
            table_name=temporary_table_name,
            auto_create_table=True,
            table_type="temporary",
            use_logical_type=True,
        )
        qc = cls.from_snowflake(temporary_table_name, enforce_ordering=True)
        return cls._post_process_file(qc, filetype="csv", **kwargs)

    @classmethod
    def from_file_with_snowflake(
        cls,
        filetype: SnowflakeSupportedFileTypeLit,
        path: str,
        **kwargs: Any,
    ) -> "SnowflakeQueryCompiler":
        """
        Returns a SnowflakeQueryCompiler whose internal frame holds the data read from
        a file or multiple files.

        If the specified file(s) are found locally, they will be uploaded to a
        stage in Snowflake and parsed there.

        See details of parameters and examples in frontend layer:
        src/snowflake/snowpark/modin/frontend/io.py
        """

        stage_location = path

        session = pd.session

        if is_local_filepath(path):
            snowpandas_prefix = "SNOWPARK_PANDAS"
            stage_prefix = generate_random_alphanumeric()
            stage_name = session.get_session_stage()
            stage_location = f"{stage_name}/{snowpandas_prefix}/{stage_prefix}"
            upload_local_path_to_snowflake_stage(session, path, stage_location)

        snowpark_reader_kwargs = get_non_pandas_kwargs(kwargs)

        # INFER_SCHEMA must always be true as it is not possible as
        # users would need to pass in both column names and their
        # data types to constitute a manually provided schema.
        snowpark_reader_kwargs["INFER_SCHEMA"] = True
        try:
            snowpark_df: SnowparkDataFrame = getattr(
                session.read.options(snowpark_reader_kwargs), filetype
            )(stage_location)
        except FileNotFoundError:
            # Return empty dataframe, Snowpark uses FileNotFoundError to indicate both missing file and
            # empty file. Staging above would detect missing file, so return empty dataframe here.
            return SnowflakeQueryCompiler.from_pandas(native_pd.DataFrame())

        # TODO: SNOW-937665
        # Unsupported Column Name '$1' when saving a Snowpark Dataframe to Snowflake.
        if snowpark_df.columns == ["$1"]:
            snowpark_df = snowpark_df.rename("$1", "COLUMN1")  # pragma: no cover

        temporary_table_name = random_name_for_temp_object(TempObjectType.TABLE)

        # TODO: SNOW-1045261 Pull save_as_table function into OrderedDataFrame so we don't have to set statement_params
        # here
        snowpark_df.write.save_as_table(
            temporary_table_name,
            mode="errorifexists",
            table_type="temporary",
            statement_params=get_default_snowpark_pandas_statement_params(),
        )

        qc = cls.from_snowflake(
            name_or_query=temporary_table_name, enforce_ordering=True
        )

        return cls._post_process_file(qc=qc, filetype=filetype, **kwargs)

    @classmethod
    def _post_process_file(
        cls,
        qc: "SnowflakeQueryCompiler",
        filetype: SnowflakeSupportedFileTypeLit,
        **kwargs: Any,
    ) -> "SnowflakeQueryCompiler":
        """
        Performs final porocessing of a file and returns a SnowflakeQueryCompiler. When
        reading files into Snowpark pandas we need perform some work after the table has
        been loaded for certain arguments, specifically header names, dtypes, and usecols.
        These parameters can be given arguments which are not currently supported by
        snowflake or they can use positional references.
        """
        if not kwargs.get("parse_header", True):
            # Rename df header since default header in pandas is
            # 0, 1, 2, ... n.  while default header in SF is c1, c2, ... cn.
            columns_renamed = {
                column_name: index for index, column_name in enumerate(qc.columns)
            }
            qc = qc.rename(columns_renamer=columns_renamed)

        dtype_ = kwargs.get("dtype", None)
        if dtype_ is not None:
            if not isinstance(dtype_, dict):
                dtype_ = {column: dtype_ for column in qc.columns}

            qc = qc.astype(dtype_)

        names = kwargs.get("names", no_default)
        if names is not no_default and names is not None:
            pandas.io.parsers.readers._validate_names(names)
            if len(names) > len(qc.columns):
                raise ValueError(
                    f"Too many columns specified: expected {len(names)} and found {len(qc.columns)}"
                )

            # Transform unnamed data columns into an index/multi-index column(s).
            if len(names) < len(qc.columns):
                unnamed_indexes = [
                    column for column in qc.columns[: len(qc.columns) - len(names)]
                ]
                qc = qc.set_index(unnamed_indexes).set_index_names(
                    [None] * len(unnamed_indexes)
                )

            # Apply names to the rightmost columns.
            columns_renamer = {}

            for idx, column in enumerate(qc.columns[len(qc.columns) - len(names) :]):
                columns_renamer[column] = names[idx]

            qc = qc.rename(columns_renamer=columns_renamer)

        usecols = kwargs.get("usecols", None)

        if usecols is not None:
            maintain_usecols_order = filetype != "csv"
            frame = create_frame_with_data_columns(
                qc._modin_frame,
                get_columns_to_keep_for_usecols(
                    usecols, qc.columns, maintain_usecols_order
                ),
            )
            qc = SnowflakeQueryCompiler(frame)

        index_col = kwargs.get("index_col", None)
        if index_col:
            pandas_labeled_index_cols = []
            input_index_cols = index_col
            if is_scalar(index_col):
                input_index_cols = [index_col]
            for column in input_index_cols:
                if isinstance(column, str):
                    if column not in qc.columns:
                        raise ValueError(f"Index {column} invalid")
                    pandas_labeled_index_cols.append(column)
                elif isinstance(column, int):
                    if column < 0:
                        column += len(qc.columns)

                    if column not in range(len(qc.columns)):
                        raise IndexError("list index out of range")
                    pandas_labeled_index_cols.append(qc.columns[column])
                else:
                    raise TypeError(
                        f"list indices must be integers or slices, not {type(column).__name__}"
                    )

            if len(set(pandas_labeled_index_cols)) != len(pandas_labeled_index_cols):
                raise ValueError("Duplicate columns in index_col are not allowed.")

            if len(pandas_labeled_index_cols) != 0:
                qc = qc.set_index(pandas_labeled_index_cols)  # type: ignore[arg-type]
        return qc

    def _to_snowpark_dataframe_from_snowpark_pandas_dataframe(
        self,
        index: bool = True,
        index_label: Optional[IndexLabel] = None,
        data_column_labels: Optional[List[Hashable]] = None,
    ) -> SnowparkDataFrame:
        """
        Convert the Snowpark pandas Dataframe to Snowpark Dataframe. The Snowpark Dataframe is created by selecting
        all index columns of the Snowpark pandas Dataframe if index=True, and also all data columns
        if data_column_labels is None.
        For example:
        With a Snowpark pandas Dataframe (df) has index=[`A`, `B`], columns = [`C`, `D`],
        the result Snowpark Dataframe after calling _to_snowpark_dataframe_from_snowpark_pandas_dataframe(index=True),
        will have columns [`A`, `B`, `C`, `D`].

        Checks are performed for pandas labels that will lead to invalid Snowflake identifiers. Example of pandas
        labels that can result in invalid Snowflake identifiers are None and duplicated labels.

        Note that Once converted to Snowpark Dataframe, ordering information will be lost, and there is no ordering
        guarantee when displaying the Snowpark Dataframe result.

        Args:
            index: bool, default True
                whether to include the index column in the final dataframe
            index_label: Optional[IndexLabel], default None
                the new label used for the index columns, the length must be the same as the number of index column
                of the current dataframe. If None, the original index name is used.
            data_column_labels: Optional[Hashable], default None
                Data columns to include. If none include all data columns.

        Returns:
            SnowparkDataFrame
                The SnowparkDataFrame contains index columns if retained (index=True) and all data columns
        Raises:
            ValueError if duplicated labels occur among the index and data columns because snowflake doesn't allow
                    duplicated identifiers.
            ValueError if index/data column label is None, because snowflake column requires a column identifier.
        """

        frame = self._modin_frame
        index_column_labels = []
        if index:
            # Include index columns
            if index_label:
                index_column_labels = (
                    index_label if isinstance(index_label, list) else [index_label]
                )
                if len(index_column_labels) != self._modin_frame.num_index_columns:
                    raise ValueError(
                        f"Length of 'index_label' should match number of levels, which is {self._modin_frame.num_index_columns}"
                    )
            else:
                index_column_labels = frame.index_column_pandas_labels

            if any(
                is_all_label_components_none(label) for label in index_column_labels
            ):
                frame = self.reset_index()._modin_frame
                index = False
                index_column_labels = []

        if data_column_labels is None:
            data_column_labels = frame.data_column_pandas_labels

        if frame.is_unnamed_series():
            # this is an unnamed Snowpark pandas series, there is no customer visible pandas
            # label for the data column, set the label to be None
            data_column_labels = [None]

        # check if there is any data column label is none
        if any(is_all_label_components_none(label) for label in data_column_labels):
            raise ValueError(
                f"Label None is found in the data columns {data_column_labels}, which is invalid in Snowflake. "
                "Please give it a name by set the dataframe columns like df.columns=['A', 'B'],"
                " or set the series name if it is a series like series.name='A'."
            )

        # perform a column name duplication check
        index_and_data_columns = data_column_labels + index_column_labels
        duplicates = extract_all_duplicates(index_and_data_columns)
        if duplicates:
            raise ValueError(
                f"Duplicated labels {duplicates} found in index columns {index_column_labels} and data columns {data_column_labels}. "
                f"Snowflake does not allow duplicated identifiers, please rename to make sure there is no duplication "
                f"among both index and data columns."
            )

        # rename snowflake quoted identifiers for the retained index columns and data columns to
        # be the same as quoted pandas labels.
        rename_mapper: dict[str, str] = {}
        identifiers_to_retain: list[str] = []
        # if index is true, retain both index + data column identifiers in order, otherwise, only retain
        # the data column identifiers
        if index:
            identifiers_to_retain.extend(
                frame.index_column_snowflake_quoted_identifiers
            )
        identifiers_to_retain.extend(
            [
                t[0]
                for t in frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                    data_column_labels, include_index=False
                )
            ]
        )
        for pandas_label, snowflake_identifier in zip(
            index_column_labels + data_column_labels,
            identifiers_to_retain,
        ):
            snowflake_quoted_identifier_to_save = quote_name_without_upper_casing(
                f"{pandas_label}"
            )
            rename_mapper[snowflake_identifier] = snowflake_quoted_identifier_to_save

        # first do a select to project out all unnecessary columns, then rename to avoid conflict
        ordered_dataframe = frame.ordered_dataframe.select(identifiers_to_retain)

        return ordered_dataframe.to_projected_snowpark_dataframe(
            col_mapper=rename_mapper
        )

    def to_csv_with_snowflake(self, **kwargs: Any) -> None:
        """
        Write data to a csv file in snowflake stage.
        Args:
            **kwargs: to_csv arguments.
        """
        self._raise_not_implemented_error_for_timedelta()

        # Raise not implemented error for unsupported parameters.
        unsupported_params = [
            "float_format",
            "mode",
            "encoding",
            "quoting",
            "quotechar",
            "lineterminator",
            "doublequote",
            "decimal",
        ]
        for param in unsupported_params:
            if kwargs.get(param) is not TO_CSV_DEFAULTS[param]:
                ErrorMessage.parameter_not_implemented_error(param, "to_csv")

        ignored_params = ["chunksize", "errors", "storage_options"]
        for param in ignored_params:
            if kwargs.get(param) is not TO_CSV_DEFAULTS[param]:
                WarningMessage.ignored_argument("to_csv", param, "")

        def _get_param(param_name: str) -> Any:
            """
            Extract parameter value from kwargs. If missing return default value.
            """
            return kwargs.get(param_name, TO_CSV_DEFAULTS[param_name])

        path = _get_param("path_or_buf")
        compression = get_compression_algorithm_for_csv(_get_param("compression"), path)

        index = _get_param("index")
        snowpark_df = self._to_snowpark_dataframe_from_snowpark_pandas_dataframe(
            index, _get_param("index_label"), _get_param("columns")
        )
        na_sep = _get_param("na_rep")
        snowpark_df.write.csv(
            location=path,
            format_type_options={
                "COMPRESSION": compression if compression else "NONE",
                "FIELD_DELIMITER": _get_param("sep"),
                "NULL_IF": na_sep if na_sep else (),
                "ESCAPE": _get_param("escapechar"),
                "DATE_FORMAT": _get_param("date_format"),
                "EMPTY_FIELD_AS_NULL": False,
            },
            header=_get_param("header"),
            single=True,
            statement_params=get_default_snowpark_pandas_statement_params(),
        )

    def to_snowflake(
        self,
        name: Union[str, Iterable[str]],
        if_exists: Optional[Literal["fail", "replace", "append"]] = "fail",
        index: bool = True,
        index_label: Optional[IndexLabel] = None,
        table_type: Literal["", "temp", "temporary", "transient"] = "",
    ) -> None:
        self._warn_lost_snowpark_pandas_type()

        if if_exists not in ("fail", "replace", "append"):
            # Same error message as native pandas.
            raise ValueError(f"'{if_exists}' is not valid for if_exists")
        if if_exists == "fail":
            mode = "errorifexists"
        elif if_exists == "replace":
            mode = "overwrite"
        else:
            mode = "append"

        if mode == "errorifexists" and pd.session._table_exists(
            parse_table_name(name) if isinstance(name, str) else name
        ):
            raise ValueError(
                f"Table '{name}' already exists. Set 'if_exists' parameter as 'replace' to override existing table."
            )

        self._to_snowpark_dataframe_from_snowpark_pandas_dataframe(
            index, index_label
        ).write.save_as_table(
            name,
            mode=mode,
            table_type=table_type,
            statement_params=get_default_snowpark_pandas_statement_params(),
        )

    def to_snowpark(
        self, index: bool = True, index_label: Optional[IndexLabel] = None
    ) -> SnowparkDataFrame:
        """
        Convert the Snowpark pandas Dataframe to Snowpark Dataframe. The Snowpark Dataframe is created by selecting
        all index columns of the Snowpark pandas Dataframe if index=True, and also all data columns.
        For example:
        With a Snowpark pandas Dataframe (df) has index=[`A`, `B`], columns = [`C`, `D`],
        the result Snowpark Dataframe after calling _to_snowpark_dataframe_from_snowpark_pandas_dataframe(index=True),
        will have columns [`A`, `B`, `C`, `D`].

        Checks are performed for pandas labels that will lead to invalid Snowflake identifiers. Example of pandas
        labels that can result in invalid Snowflake identifiers are None and duplicated labels.

        Note that Once converted to Snowpark Dataframe, ordering information will be lost, and there is no ordering
        guarantee when displaying the Snowpark Dataframe result.

        For details, please see comment in _to_snowpark_dataframe_of_pandas_dataframe.
        """
        self._warn_lost_snowpark_pandas_type()

        return self._to_snowpark_dataframe_from_snowpark_pandas_dataframe(
            index, index_label
        )

    @snowpark_pandas_type_immutable_check
    def cache_result(self) -> "SnowflakeQueryCompiler":
        """
        Returns a materialized view of this QueryCompiler.
        """
        return SnowflakeQueryCompiler(self._modin_frame.persist_to_temporary_table())

    @snowpark_pandas_type_immutable_check
    def set_columns(self, new_pandas_labels: Axes) -> "SnowflakeQueryCompiler":
        """
        Set pandas column labels with the new column labels

        Args:
            new_pandas_labels: A list like or index containing new pandas column names

        Returns:
            a new `SnowflakeQueryCompiler` with updated column labels
        """
        # new_pandas_names should be able to convert into an index which is consistent to pandas df.columns behavior
        from snowflake.snowpark.modin.plugin.extensions.utils import (
            try_convert_index_to_native,
        )

        new_pandas_labels = ensure_index(try_convert_index_to_native(new_pandas_labels))
        if len(new_pandas_labels) != len(self._modin_frame.data_column_pandas_labels):
            raise ValueError(
                "Length mismatch: Expected axis has {} elements, new values have {} elements".format(
                    len(self._modin_frame.data_column_pandas_labels),
                    len(new_pandas_labels),
                )
            )

        # Rename data columns in Snowpark dataframe. This step is not needed for correctness, we rename
        # underlying Snowpark columns to keep them as close as possible to pandas labels. This is helpful for
        # debuggability.
        new_data_column_snowflake_quoted_identifiers = (
            self._modin_frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
                pandas_labels=new_pandas_labels.tolist(),
            )
        )
        renamed_quoted_identifier_mapping = dict(
            zip(
                self._modin_frame.data_column_snowflake_quoted_identifiers,
                new_data_column_snowflake_quoted_identifiers,
            )
        )

        renamed_frame = self._modin_frame.rename_snowflake_identifiers(
            renamed_quoted_identifier_mapping
        )

        new_internal_frame = InternalFrame.create(
            ordered_dataframe=renamed_frame.ordered_dataframe,
            data_column_pandas_labels=new_pandas_labels.tolist(),
            data_column_pandas_index_names=new_pandas_labels.names,
            data_column_snowflake_quoted_identifiers=new_data_column_snowflake_quoted_identifiers,
            index_column_pandas_labels=renamed_frame.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=renamed_frame.index_column_snowflake_quoted_identifiers,
            data_column_types=renamed_frame.cached_data_column_snowpark_pandas_types,
            index_column_types=renamed_frame.cached_index_column_snowpark_pandas_types,
        )
        return SnowflakeQueryCompiler(new_internal_frame)

    # TODO SNOW-837664: add more tests for df.columns
    def get_columns(self) -> native_pd.Index:
        return self._modin_frame.data_columns_index

    columns: native_pd.Index = property(get_columns, set_columns)

    def _shift_values(
        self, periods: int, axis: Union[Literal[0], Literal[1]], fill_value: Hashable
    ) -> "SnowflakeQueryCompiler":
        """
        Implements logic to shift data of DataFrame or Series.
        Args:
            periods: periods by which to shift
            axis: along which axis to shift rows (axis=0) or columns (axis=1)
            fill_value: value to fill new columns with.

        Returns:
            SnowflakeQueryCompiler
        """

        if axis == 0:
            return self._shift_values_axis_0(periods, fill_value)
        else:
            return self._shift_values_axis_1(periods, fill_value)

    def _shift_values_axis_0(
        self, periods: int, fill_value: Hashable
    ) -> "SnowflakeQueryCompiler":
        """
        Shift rows and fill new columns with fill_value.
        Args:
            periods: How many rows to shift down (periods > 0) or up (periods < 0). periods = 0 results
            in a no-op.
            fill_value: value to fill new columns with, default: NULL

        Returns:
            SnowflakeQueryCompiler
        """
        # Shift using LAG window operation over row position window together with fill_value.
        frame = self._modin_frame.ensure_row_position_column()
        row_position_quoted_identifier = frame.row_position_snowflake_quoted_identifier

        timedelta_invalid_fill_value_error_message = f"value should be a 'Timedelta' or 'NaT'. Got '{type(fill_value).__name__}' instead."

        def shift_expression_and_type(
            quoted_identifier: str, dtype: DataType
        ) -> SnowparkPandasColumn:
            """
            Helper function to generate lag-based shift expression for Snowpark pandas. Performs
            necessary type conversion if datatype of fill_value is not compatible with a column's datatype.
            Args:
                quoted_identifier: identifier of column for which to generate shift expression
                dtype: datatype of column identified by quoted_identifier

            Returns:
                SnowparkPandasColumn representing the result.
            """
            if isinstance(dtype, TimedeltaType):
                if isinstance(fill_value, str):
                    # Despite the error messages, pandas allows filling a timedelta
                    # with strings, but it converts strings to timedelta.
                    try:
                        fill_value_for_snowpark = pd.Timedelta(fill_value)
                    except BaseException:
                        raise TypeError(timedelta_invalid_fill_value_error_message)
                else:
                    fill_value_for_snowpark = fill_value
                if not (
                    pd.isna(fill_value_for_snowpark)
                    or isinstance(
                        SnowparkPandasType.get_snowpark_pandas_type_for_pandas_type(
                            type(fill_value_for_snowpark)
                        ),
                        TimedeltaType,
                    )
                ):
                    raise TypeError(timedelta_invalid_fill_value_error_message)
            else:
                fill_value_for_snowpark = fill_value

            fill_value_dtype = infer_object_type(fill_value_for_snowpark)
            fill_value_snowpark_column = (
                None
                if pd.isna(fill_value_for_snowpark)
                else pandas_lit(fill_value_for_snowpark)
            )

            window_expr = Window.orderBy(col(row_position_quoted_identifier))

            # convert to variant type if types differ
            if fill_value_snowpark_column is not None and dtype != fill_value_dtype:
                shift_expression = lag(
                    to_variant(col(quoted_identifier)),
                    offset=periods,
                    default_value=to_variant(fill_value_snowpark_column),
                ).over(window_expr)
                expression_type = VariantType()
            else:
                shift_expression = lag(
                    quoted_identifier,
                    offset=periods,
                    default_value=fill_value_snowpark_column,
                ).over(window_expr)
                expression_type = dtype
            # TODO(https://snowflakecomputing.atlassian.net/browse/SNOW-1634393):
            # Prevent ourselves from using types that are DataType but not
            # SnowparkPandasType. In this particular case, the type should
            # indeed be Optional[SnowparkPandasType]
            return (
                shift_expression,
                expression_type
                if isinstance(expression_type, SnowparkPandasType)
                else None,
            )

        quoted_identifier_to_column_map = {}
        data_column_snowpark_pandas_types = []
        for identifier in frame.data_column_snowflake_quoted_identifiers:
            expression, snowpark_pandas_type = shift_expression_and_type(
                identifier, frame.get_snowflake_type(identifier)
            )
            quoted_identifier_to_column_map[identifier] = expression
            data_column_snowpark_pandas_types.append(snowpark_pandas_type)
        new_frame = frame.update_snowflake_quoted_identifiers_with_expressions(
            quoted_identifier_to_column_map=quoted_identifier_to_column_map,
            snowpark_pandas_types=data_column_snowpark_pandas_types,
        ).frame

        return self.__constructor__(new_frame)

    def _shift_values_axis_1(
        self, periods: int, fill_value: Hashable
    ) -> "SnowflakeQueryCompiler":
        """
        Shift columns and fill new columns with fill_value.
        Args:
            periods: How many columns to shift to the right (periods > 0) or left (periods < 0). periods = 0 results
            in a no-op.
            fill_value: value to fill new columns with, default: NULL

        Returns:
            SnowflakeQueryCompiler
        """

        frame = self._modin_frame
        column_labels = frame.data_column_pandas_labels

        fill_value_snowpark_pandas_type = (
            SnowparkPandasType.get_snowpark_pandas_type_for_pandas_type(
                type(fill_value)
            )
        )

        # Fill all columns with fill value (or NULL) if abs(periods) exceeds column count.
        if abs(periods) >= len(column_labels):
            new_frame = frame.apply_snowpark_function_to_columns(
                lambda column: pandas_lit(fill_value),
                return_type=fill_value_snowpark_pandas_type,
            )
            return self.__constructor__(new_frame)

        # No fill with fill value when using periods == 0. Can be handled in frontend as well,
        # listed here for completeness.
        if periods == 0:  # pragma: no cover
            return self  # pragma: no cover

        # Positive periods shift to the right, negative periods shift to the left
        # note that the order of data_column_snowflake_quoted_identifiers is the same as data_column_pandas_labels,
        # therefore we can directly operate on data_column_snowflake_quoted_identifiers
        col_expressions = [
            col(quoted_identifier)
            for quoted_identifier in frame.data_column_snowflake_quoted_identifiers
        ]
        col_snowpark_pandas_types = frame.cached_data_column_snowpark_pandas_types
        if periods > 0:
            # create expressions to shift data to right
            # | lit(...) | lit(...) | ... | lit(...) | col(...) | ... | col(...) |
            col_expressions = [pandas_lit(fill_value)] * periods + col_expressions[
                :-periods
            ]
            snowpark_pandas_types = [
                fill_value_snowpark_pandas_type
            ] * periods + col_snowpark_pandas_types[:-periods]
        else:
            # create expressions to shift data to left
            # | col(...) | ... | col(...) | lit(...) | lit(...) | ... | lit(...) |
            col_expressions = col_expressions[-periods:] + [pandas_lit(fill_value)] * (
                -periods
            )
            snowpark_pandas_types = col_snowpark_pandas_types[-periods:] + [
                fill_value_snowpark_pandas_type
            ] * (-periods)

        new_frame = frame.update_snowflake_quoted_identifiers_with_expressions(
            {
                quoted_identifier: col_expressions[i]
                for i, quoted_identifier in enumerate(
                    frame.data_column_snowflake_quoted_identifiers
                )
            },
            snowpark_pandas_types=snowpark_pandas_types,
        ).frame

        return self.__constructor__(new_frame)

    def _shift_index(self, periods: int, freq: Any) -> "SnowflakeQueryCompiler":  # type: ignore[return]
        """
        Shift index, to be implemented in SNOW-1023324.
        Args:
            periods: By what period to shift index (multiple of freq)
            freq: frequency to use, revisit type hint Any as part of ticket to restrict.

        Returns:
            SnowflakeQueryCompiler
        """

        assert freq is not None, "freq must be specified when calling shift index"

        # TODO: SNOW-1023324, implement shifting index only.
        ErrorMessage.not_implemented("shifting index values not yet supported.")

    def shift(
        self,
        periods: Union[int, Sequence[int]] = 1,
        freq: Any = None,
        axis: Literal[0, 1] = 0,
        fill_value: Hashable = no_default,
        suffix: Optional[str] = None,
    ) -> "SnowflakeQueryCompiler":
        """
        Implements shift operation for DataFrame/Series.
        Args:
            periods: How many periods to shift for.
            freq: If given, do not shift values but index only. If None, shift only data and keep index as-is.
            axis: Whether to shift values (freq must be None) row-wise (axis=0) or column-wise (axis=1).
            fill_value: Fill new columns with this value, default: None mapped to NULL.

        Returns:
            SnowflakeQueryCompiler
        """
        if suffix is not None:
            ErrorMessage.not_implemented(
                "Snowpark pandas DataFrame/Series.shift does not yet support the `suffix` parameter"
            )
        if not isinstance(periods, int):
            ErrorMessage.not_implemented(
                "Snowpark pandas DataFrame/Series.shift does not yet support `periods` that are sequences. Only int `periods` are supported."
            )
        # if frequency is None, shift data by periods
        # else if frequency is given, shift index only
        if freq is None:
            # mypy isn't smart enough to recognize that periods is an int here
            return self._shift_values(periods, axis, fill_value)  # type: ignore
        else:
            # axis parameter ignored, should be 0 for manipulating index. Revisit in SNOW-1023324
            return self._shift_index(periods, freq)  # type: ignore  # pragma: no cover

    @property
    def index(self) -> Union["pd.Index", native_pd.MultiIndex]:
        """
        Get index. If MultiIndex, the method eagerly pulls the values from Snowflake because index requires the values
        to be filled and returns a pandas MultiIndex. If not MultiIndex, create a modin index and pass itself

        Returns:
            The index (row labels) of the DataFrame.
        """
        if self.is_multiindex():
            # Lazy multiindex is not supported
            logging.warning(
                "Lazy MultiIndex is not supported. MultiIndex values are evaluated eagerly and pulled out of Snowflake."
            )
            return self._modin_frame.index_columns_pandas_index()
        else:
            return pd.Index(query_compiler=self)

    def set_index(
        self,
        keys: list[Union[Hashable, "SnowflakeQueryCompiler"]],
        drop: Optional[bool] = True,
        append: Optional[bool] = False,
    ) -> "SnowflakeQueryCompiler":
        """
        This the implementation for DataFrame set_index API
        Args:
            keys: can be either a label/hashable, or SnowflakeQueryCompiler
            drop: same as the drop argument for df.set_index
            append: same as the append argument for df.set_index

        Returns:
            The new SnowflakeQueryCompiler after the set_index operation

        """
        if not any(isinstance(k, SnowflakeQueryCompiler) for k in keys):
            return self.set_index_from_columns(keys, drop=drop, append=append)

        new_qc = self
        for key in keys:
            if isinstance(key, SnowflakeQueryCompiler):
                new_qc = new_qc.set_index_from_series(key, append)
            else:
                new_qc = new_qc.set_index_from_columns([key], drop, append)
            append = True

        return new_qc

    def set_index_from_series(
        self,
        key: "SnowflakeQueryCompiler",
        append: Optional[bool] = False,
    ) -> "SnowflakeQueryCompiler":
        """
        The helper method implements set_index with a single series key. The basic idea is to join this series and use
        it as a new index column
        Args:
            key: the SnowflakeQueryCompiler of the series
            append: as same as append argument in set_index

        Returns:
            The new SnowflakeQueryCompiler after the set_index operation
        """

        assert (
            len(key._modin_frame.data_column_pandas_labels) == 1
        ), "need to be a series"
        self_frame = self._modin_frame.ensure_row_position_column()
        other_frame = key._modin_frame.ensure_row_position_column()

        # TODO: SNOW-935748 improve the workaround below for MultiIndex names
        # The original index names. This value is used instead of the new internal frames'
        # index names to preserve the MultiIndex columns of a DataFrame on which join() is performed.
        # Without this, the column's datatype is changed from MultiIndex to Index during the join.
        # This behavior is seen in DataFrame.set_axis() on a DataFrame with MultiIndex columns.
        index_names = self._modin_frame.data_column_pandas_index_names

        new_internal_frame, result_column_mapper = join_utils.join(
            self_frame,
            other_frame,
            how="left",
            left_on=[self_frame.row_position_snowflake_quoted_identifier],
            right_on=[other_frame.row_position_snowflake_quoted_identifier],
            inherit_join_index=InheritJoinIndex.FROM_LEFT,
        )

        series_name = key._modin_frame.data_column_pandas_labels[0]
        if series_name == MODIN_UNNAMED_SERIES_LABEL:
            series_name = None
        new_index_labels = [series_name]
        new_index_ids = result_column_mapper.map_right_quoted_identifiers(
            other_frame.data_column_snowflake_quoted_identifiers
        )
        new_index_snowpark_types = other_frame.cached_data_column_snowpark_pandas_types
        if append:
            new_index_labels = (
                new_internal_frame.index_column_pandas_labels + new_index_labels
            )
            new_index_ids = (
                new_internal_frame.index_column_snowflake_quoted_identifiers
                + new_index_ids
            )
            new_index_snowpark_types = (
                self_frame.cached_index_column_snowpark_pandas_types
                + new_index_snowpark_types
            )
        new_internal_frame = InternalFrame.create(
            ordered_dataframe=new_internal_frame.ordered_dataframe,
            data_column_pandas_labels=self_frame.data_column_pandas_labels,
            data_column_pandas_index_names=index_names,
            data_column_snowflake_quoted_identifiers=result_column_mapper.map_left_quoted_identifiers(
                self_frame.data_column_snowflake_quoted_identifiers
            ),
            index_column_pandas_labels=new_index_labels,
            index_column_snowflake_quoted_identifiers=new_index_ids,
            data_column_types=self_frame.cached_data_column_snowpark_pandas_types,
            index_column_types=new_index_snowpark_types,
        )
        return SnowflakeQueryCompiler(new_internal_frame)

    def get_index_names(self, axis: int = 0) -> list[Hashable]:
        """
        Get index names of specified axis.

        Parameters
        ----------
        axis : {0, 1}, default: 0
        Axis to get index names on.

        Returns
        -------
        list names for the Index along the direction.
        """
        return (
            self._modin_frame.index_column_pandas_labels
            if axis == 0
            else self._modin_frame.data_column_pandas_index_names
        )

    def _binary_op_scalar_rhs(
        self, op: str, other: Scalar, fill_value: Scalar
    ) -> "SnowflakeQueryCompiler":
        """
        Perform binary operation between a Series/DataFrame and a scalar.

        Args:
            op: Name of binary operation.
            other: Second operand of binary operation, a list-like object.
            fill_value: Fill existing missing (NaN) values, and any new element needed for
                successful DataFrame alignment, with this value before computation.
                If data in both corresponding DataFrame locations is missing the result will be missing.
                only arithmetic binary operation has this parameter (e.g., add() has, but eq() doesn't have).
        """
        replace_mapping = {}
        data_column_snowpark_pandas_types = []
        for identifier in self._modin_frame.data_column_snowflake_quoted_identifiers:
            expression, snowpark_pandas_type = BinaryOp.create_with_fill_value(
                op=op,
                lhs=col(identifier),
                lhs_datatype=lambda identifier=identifier: self._modin_frame.get_snowflake_type(
                    identifier
                ),
                rhs=pandas_lit(other),
                rhs_datatype=lambda: infer_object_type(other),
                fill_value=fill_value,
            ).compute()
            replace_mapping[identifier] = expression
            data_column_snowpark_pandas_types.append(snowpark_pandas_type)
        return SnowflakeQueryCompiler(
            self._modin_frame.update_snowflake_quoted_identifiers_with_expressions(
                quoted_identifier_to_column_map=replace_mapping,
                snowpark_pandas_types=data_column_snowpark_pandas_types,
            ).frame
        )

    def _binary_op_list_like_rhs_axis_0(
        self,
        op: str,
        other: AnyArrayLike,
        fill_value: Scalar,
    ) -> "SnowflakeQueryCompiler":
        """
        Perform binary operation between a Series/DataFrame and a list-like object on axis=0.

        Args:
            op: Name of binary operation.
            other: Second operand of binary operation, a list-like object.
            fill_value: Fill existing missing (NaN) values, and any new element needed for
                successful DataFrame alignment, with this value before computation.
                If data in both corresponding DataFrame locations is missing the result will be missing.
                only arithmetic binary operation has this parameter (e.g., add() has, but eq() doesn't have).
        """
        from modin.pandas import Series

        # Step 1: Convert other to a Series and join on the row position with self.
        other_qc = Series(other)._query_compiler
        self_frame = self._modin_frame.ensure_row_position_column()
        other_frame = other_qc._modin_frame.ensure_row_position_column()
        new_frame = join_utils.align(
            left=self_frame,
            right=other_frame,
            left_on=[self_frame.row_position_snowflake_quoted_identifier],
            right_on=[other_frame.row_position_snowflake_quoted_identifier],
            how="coalesce",
        ).result_frame

        # Step 2: The operation will be performed as a broadcast operation over all columns, therefore iterate
        # through all the data quoted identifiers. In the case of a Series, there is only one data column.

        # Due to the join above, other's data column is the right-most column.
        other_identifier = new_frame.data_column_snowflake_quoted_identifiers[-1]
        # Step 3: Create a map from the column identifier to the binary operation expression. This is used
        # to update the column data.
        replace_mapping = {}
        snowpark_pandas_types = []
        for identifier in new_frame.data_column_snowflake_quoted_identifiers[:-1]:
            expression, snowpark_pandas_type = BinaryOp.create_with_fill_value(
                op=op,
                lhs=col(identifier),
                lhs_datatype=lambda identifier=identifier: new_frame.get_snowflake_type(
                    identifier
                ),
                rhs=col(other_identifier),
                rhs_datatype=lambda: new_frame.get_snowflake_type(other_identifier),
                fill_value=fill_value,
            ).compute()
            replace_mapping[identifier] = expression
            snowpark_pandas_types.append(snowpark_pandas_type)

        # Step 4: Update the frame with the expressions map and return a new query compiler after removing the
        # column representing other's data.
        new_frame = new_frame.update_snowflake_quoted_identifiers_with_expressions(
            replace_mapping
        ).frame
        new_frame = InternalFrame.create(
            ordered_dataframe=new_frame.ordered_dataframe,
            data_column_pandas_labels=new_frame.data_column_pandas_labels[:-1],
            data_column_snowflake_quoted_identifiers=new_frame.data_column_snowflake_quoted_identifiers[
                :-1
            ],
            data_column_pandas_index_names=new_frame.data_column_pandas_index_names,
            index_column_pandas_labels=new_frame.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=new_frame.index_column_snowflake_quoted_identifiers,
            data_column_types=snowpark_pandas_types,
            index_column_types=None,
        )
        return SnowflakeQueryCompiler(new_frame)

    def _binary_op_list_like_rhs_axis_1(
        self,
        op: str,
        other: AnyArrayLike,
        fill_value: Scalar,
    ) -> "SnowflakeQueryCompiler":
        """
        Perform binary operation between a DataFrame and a list-like object on axis=1.

        Args:
            op: Name of binary operation.
            other: Second operand of binary operation, a list-like object.
            fill_value: Fill existing missing (NaN) values, and any new element needed for
                successful DataFrame alignment, with this value before computation.
                If data in both corresponding DataFrame locations is missing the result will be missing.
                only arithmetic binary operation has this parameter (e.g., add() has, but eq() doesn't have).
        """
        from modin.pandas.utils import is_scalar

        replace_mapping = {}  # map: column identifier -> column expression
        # Convert list-like object to list since the NaN values in the rhs are treated as invalid identifiers
        # (misinterpreted SQL query) when the list-like object is not a list.
        # Error: SnowparkSQLException: compilation error: error line 1 at position 313 invalid identifier 'NAN'.
        other = other.tolist() if not isinstance(other, list) else other

        # each element in the list-like object can be treated as a scalar for each corresponding column.
        snowpark_pandas_types = []
        for idx, identifier in enumerate(
            self._modin_frame.data_column_snowflake_quoted_identifiers
        ):
            # iterate through `other` and use each element on a column.
            # 1. if len(rhs) > num_cols, ignore the extra rhs elements.
            # 2. if len(rhs) < num_cols, substitute missing elements with None.
            lhs = col(identifier)
            rhs = other[idx] if idx < len(other) else None
            rhs = None if rhs == np.nan else rhs

            # rhs is not guaranteed to be a scalar value - it can be a list-like as well.
            # Convert all list-like objects to a list.
            rhs_lit = pandas_lit(rhs) if is_scalar(rhs) else pandas_lit(rhs.tolist())
            expression, snowpark_pandas_type = BinaryOp.create_with_fill_value(
                op,
                lhs=lhs,
                lhs_datatype=lambda identifier=identifier: self._modin_frame.get_snowflake_type(
                    identifier
                ),
                rhs=rhs_lit,
                rhs_datatype=lambda rhs=rhs: infer_object_type(rhs),
                fill_value=fill_value,
            ).compute()
            replace_mapping[identifier] = expression
            snowpark_pandas_types.append(snowpark_pandas_type)

        return SnowflakeQueryCompiler(
            self._modin_frame.update_snowflake_quoted_identifiers_with_expressions(
                replace_mapping, snowpark_pandas_types
            ).frame
        )

    def binary_op(
        self,
        op: str,
        other: Union[Scalar, AnyArrayLike, "pd.Series", "pd.DataFrame"],
        axis: int,
        level: Optional[Level] = None,
        fill_value: Optional[Scalar] = None,
        squeeze_self: bool = False,
        **kwargs: Any,
    ) -> "SnowflakeQueryCompiler":
        """
        Perform binary operation.

        Args:
            op: Name of binary operation.
            other: Second operand of binary operation, which can be Scalar, Series or SnowflakeQueryCompiler.
            axis: 0 (index), 1 (columns)
            level: Broadcast across a level, matching Index values on the passed MultiIndex level.
            fill_value: Fill existing missing (NaN) values, and any new element needed for
                successful DataFrame alignment, with this value before computation.
                If data in both corresponding DataFrame locations is missing the result will be missing.
                only arithmetic binary operation has this parameter (e.g., add() has, but eq() doesn't have).
            squeeze_self: If True, this query compiler comes from a Series.
        """

        # We distinguish between 5 cases here to handle an operation between the DataFrame/Series represented by this
        # SnowflakeQueryCompiler and other
        # 1. other is scalar                                        (DataFrame/Series <op> scalar)
        # 2. other is list_like                                     (DataFrame/Series <op> array)
        # 3. this is Series and other is Series                     (Series <op> Series)
        # 4. this is Series and other is DataFrame or vice-versa    (DataFrame <op> Series)
        # 5. this is DataFrame and other is DataFrame               (DataFrame <op> DataFrame)

        # Native pandas does not support binary operations between a Series and a list-like object.

        from modin.pandas import Series
        from modin.pandas.dataframe import DataFrame
        from modin.pandas.utils import is_scalar

        # fail explicitly for unsupported scenarios
        if level is not None:
            # TODO SNOW-862668: binary operations with level
            ErrorMessage.not_implemented(f"parameter level={level} not yet supported")

        if fill_value is not None:
            if not is_scalar(fill_value):
                # In native pandas, single element list-like objects can be used as fill_value, however this does not
                # match pandas documentation; hence it is omitted in the Snowpark pandas implementation.
                raise ValueError("Only scalars can be used as fill_value.")

        if not BinaryOp.is_binary_op_supported(op):
            ErrorMessage.not_implemented(
                f"Snowpark pandas doesn't yet support '{op}' binary operation"
            )

        if is_scalar(other):
            # (Case 1): other is scalar
            # -------------------------
            return self._binary_op_scalar_rhs(op, other, fill_value)

        if not isinstance(other, (Series, DataFrame)) and is_list_like(other):
            # (Case 2): other is list-like
            # ----------------------------
            if axis == 0:
                return self._binary_op_list_like_rhs_axis_0(op, other, fill_value)
            else:  # axis=1
                return self._binary_op_list_like_rhs_axis_1(op, other, fill_value)

        if squeeze_self and isinstance(other, Series):
            # (Case 3): Series/Series
            # -----------------------
            # Both series objects are joined (with an outer join) based on their index,
            # and the result is sorted after the index.
            # In addition, pandas drops the name and the result becomes an unnamed series.
            # E.g., for
            # s1 = pd.Series([1, 2, 3], index=[5, 0, 1], name='s1')
            # s2 = pd.Series([3, 5, 4], index=[1, 2, 10], name='s2')
            # The result of
            # s1 + s2
            # is
            # 0     NaN
            # 1     6.0
            # 2     NaN
            # 5     NaN
            # 10    NaN
            # dtype: float64

            lhs_frame = self._modin_frame
            rhs_frame = other._query_compiler._modin_frame

            # In native pandas when binary operation is performed between two series,
            # they are joined on row position if indices are exact match otherwise
            # they are joined with outer join.
            # For example:
            # s1 = pd.Series([1, 2, 3], index=[2, 1, 2])
            # s2 = pd.Series([1, 1, 1], index=[2, 1, 2])
            # s1 + s2 -> pd.Series([2, 3, 4], index=[2, 1, 2])
            #
            # s3 = pd.Series([1, 2, 3], index=[2, 1, 2])
            # s4 = pd.Series([1, 1, 1], index=[2, 3, 2])
            # s3 + s4 -> pd.Series([NaN, 2, 2, 4, 4, NaN], index=[1, 2, 2, 2, 2, 3])
            aligned_frame, result_column_mapper = join_utils.align_on_index(
                lhs_frame, rhs_frame
            )

            assert 2 == len(aligned_frame.data_column_snowflake_quoted_identifiers)

            lhs_quoted_identifier = result_column_mapper.map_left_quoted_identifiers(
                lhs_frame.data_column_snowflake_quoted_identifiers
            )[0]
            rhs_quoted_identifier = result_column_mapper.map_right_quoted_identifiers(
                rhs_frame.data_column_snowflake_quoted_identifiers
            )[0]

            # add new column with result as unnamed
            new_column_expr, snowpark_pandas_type = BinaryOp.create_with_fill_value(
                op=op,
                lhs=col(lhs_quoted_identifier),
                lhs_datatype=lambda: aligned_frame.get_snowflake_type(
                    lhs_quoted_identifier
                ),
                rhs=col(rhs_quoted_identifier),
                rhs_datatype=lambda: aligned_frame.get_snowflake_type(
                    rhs_quoted_identifier
                ),
                fill_value=fill_value,
            ).compute()

            # name is dropped when names of series differ. A dropped name is using unnamed series label.
            new_column_name = (
                MODIN_UNNAMED_SERIES_LABEL
                if lhs_frame.data_column_pandas_labels[0]
                != rhs_frame.data_column_pandas_labels[0]
                else lhs_frame.data_column_pandas_labels[0]
            )

            new_frame = aligned_frame.append_column(
                new_column_name, new_column_expr, value_type=snowpark_pandas_type
            )

            # return only newly created column. Because column has been appended, this is the last column indexed by -1
            return SnowflakeQueryCompiler(
                get_frame_by_col_pos(internal_frame=new_frame, columns=[-1])
            )
        elif squeeze_self or isinstance(other, Series):
            # (Case 4): Series/DataFrame or DataFrame/Series
            # --------------------------
            # Distinguish here between axis=0 and axis=1 case

            # Note that a binary operation for axis == 0 only works for
            # the case DataFrame <binop> Series. self is a DataFrame if squeeze_self is False.
            # However, pandas allows to call Series <binop> DataFrame with axis=0 set. In this case, the parameter
            # axis=0 is ignored and the result works the same as if axis=1 is invoked.
            if not squeeze_self and axis == 0:
                return self._binary_op_between_dataframe_and_series_along_axis_0(
                    op, other._query_compiler, fill_value
                )

            # Invoke axis=1 case, this is the correct pandas behavior if squeeze_self is True and axis=0 also.
            return self._binary_op_between_dataframe_and_series_along_axis_1(
                op, other._query_compiler, squeeze_self, fill_value
            )
        else:
            # (Case 5): DataFrame/DataFrame
            # -----------------------------

            # other must be DataFrame
            assert isinstance(other, DataFrame)

            # The axis parameter is ignored for DataFrame <binop> DataFrame operations. The default axis behavior
            # is always aligning by columns (axis=1). Binary operations between DataFrames support fill_value.
            return self._binary_op_between_dataframes(
                op, other._query_compiler, fill_value
            )

    def _bool_reduce_helper(
        self,
        empty_value: bool,
        agg_func: Literal["all", "any"],
        axis: int,
        _bool_only: Optional[bool],
        skipna: Optional[bool],
    ) -> "SnowflakeQueryCompiler":
        """
        Performs a boolean reduction across either axis.

        empty_value: bool
            The value returned for an empty dataframe.
        agg_func: {"all", "any"}
            The name of the aggregation to apply.
        _bool_only: Optional[bool]
            Unused, accepted for compatibility with modin frontend. If true, only boolean columns are included
            in the result; this filtering is already performed on the frontend.
        skipna: Optional[bool]
            Exclude NA/null values. If the entire row/column is NA and skipna is True, then the result will be False,
            as for an empty row/column. If skipna is False, then NA are treated as True, because these are not equal to zero.
        """
        assert agg_func in ("all", "any")

        frame = self._modin_frame
        empty_columns = len(frame.data_columns_index) == 0
        if not empty_columns and not all(
            is_bool_dtype(t) or is_integer_dtype(t) for t in self.dtypes
        ):
            # Raise error if columns are non-integer/boolean
            ErrorMessage.not_implemented(
                f"Snowpark pandas {agg_func} API doesn't yet support non-integer/boolean columns"
            )

        if axis == 1:
            # append a new column representing the reduction of all the columns
            reduce_expr = pandas_lit(empty_value)
            for col_name in frame.data_column_snowflake_quoted_identifiers:
                if agg_func == "all":
                    reduce_expr = col(col_name).cast(BooleanType()) & reduce_expr
                else:
                    reduce_expr = col(col_name).cast(BooleanType()) | reduce_expr
            new_frame = frame.append_column(MODIN_UNNAMED_SERIES_LABEL, reduce_expr)
            # return only newly created column. Because column has been appended, this is the last column indexed by -1
            return SnowflakeQueryCompiler(
                get_frame_by_col_pos(internal_frame=new_frame, columns=[-1])
            )
        else:
            assert axis == 0
            # The query compiler agg method complains if the resulting aggregation is empty, so we add a special check here.
            if empty_columns:
                # The result should be an empty series of dtype bool, which is internally represented as an
                # empty dataframe with only the MODIN_UNNAMED_SERIES_LABEL column
                return SnowflakeQueryCompiler.from_pandas(
                    native_pd.DataFrame({MODIN_UNNAMED_SERIES_LABEL: []}, dtype=bool)
                )
            # If there are now rows (but there are columns), booland_agg/boolor_agg would return NULL.
            # This behavior is handled within aggregation_utils to avoid an extra query.
            return self.agg(
                agg_func,
                axis=0,
                args=[],
                kwargs={"skipna": skipna},
            )

    def all(
        self,
        axis: int,
        bool_only: Optional[bool],
        skipna: Optional[bool],
    ) -> "SnowflakeQueryCompiler":
        return self._bool_reduce_helper(
            True, "all", axis=axis, _bool_only=bool_only, skipna=skipna
        )

    def any(
        self,
        axis: int,
        bool_only: Optional[bool],
        skipna: Optional[bool],
    ) -> "SnowflakeQueryCompiler":
        return self._bool_reduce_helper(
            False, "any", axis=axis, _bool_only=bool_only, skipna=skipna
        )

    def reindex(
        self,
        axis: int,
        labels: Union[pandas.Index, "pd.Index", list[Any], "SnowflakeQueryCompiler"],
        **kwargs: dict[str, Any],
    ) -> "SnowflakeQueryCompiler":
        """
        Align QueryCompiler data with a new index along specified axis.

        Parameters
        ----------
        axis : {0, 1}
            Axis to align labels along. 0 is for index, 1 is for columns.
        labels : list-like, SnowflakeQueryCompiler
            Index-labels to align with.
        method : {None, "backfill"/"bfill", "pad"/"ffill", "nearest"}
            Method to use for filling holes in reindexed frame.
        fill_value : scalar
            Value to use for missing values in the resulted frame.
        limit : int
        tolerance : int
        **kwargs : dict
            Serves the compatibility purpose. Does not affect the result.

        Returns
        -------
        SnowflakeQueryCompiler
            QueryCompiler with aligned axis.
        """
        if self.is_multiindex(axis=axis):
            raise NotImplementedError(
                "Snowpark pandas doesn't support `reindex` with MultiIndex"
            )
        if axis == 0:
            return self._reindex_axis_0(labels=labels, **kwargs)
        else:
            return self._reindex_axis_1(labels=labels, **kwargs)

    def is_monotonic_decreasing(self) -> "SnowflakeQueryCompiler":
        """
        Returns a QueryCompiler containing only a column that checks for monotonically
        decreasing values in the first data column of this QueryCompiler.

        Returns
        -------
        SnowflakeQueryCompiler
            QueryCompiler with column to ascertain whether data is monotonically decreasing.
        """
        return self._check_monotonic(increasing=False)

    def is_monotonic_increasing(self) -> "SnowflakeQueryCompiler":
        """
        Returns a QueryCompiler containing only a column that checks for monotonically
        increasing values in the first data column of this QueryCompiler.

        Returns
        -------
        SnowflakeQueryCompiler
            QueryCompiler with column to ascertain whether data is monotonically increasing.
        """
        return self._check_monotonic(increasing=True)

    def _check_monotonic(self, increasing: bool) -> "SnowflakeQueryCompiler":
        """
        Returns a QueryCompiler containing only a column that checks for monotonically
        decreasing or increasing values (depending on `increasing`) in the first data column of this QueryCompiler.

        Parameters
        ----------
        increasing: bool
            Whether to check for monotonically increasing or decreasing values.

        Returns
        -------
        SnowflakeQueryCompiler
            QueryCompiler with column to ascertain whether data is monotonically decreasing/increasing.
        """
        col_to_check = self._modin_frame.data_column_snowflake_quoted_identifiers[0]
        (
            new_qc,
            monotonic_increasing_snowflake_quoted_identifier,
            monotonic_decreasing_snowflake_quoted_identifier,
        ) = self._add_columns_for_monotonicity_checks(
            col_to_check=col_to_check,
            columns_to_add="increasing" if increasing else "decreasing",
        )
        data_column_snowflake_quoted_identifiers = []
        if increasing:
            data_column_snowflake_quoted_identifiers.append(
                monotonic_increasing_snowflake_quoted_identifier
            )
        else:
            data_column_snowflake_quoted_identifiers.append(
                monotonic_decreasing_snowflake_quoted_identifier
            )
        new_modin_frame = new_qc._modin_frame
        qc = SnowflakeQueryCompiler(
            InternalFrame.create(
                ordered_dataframe=new_modin_frame.ordered_dataframe.limit(
                    n=1, sort=False
                ),
                data_column_pandas_index_names=new_modin_frame.data_column_pandas_index_names,
                data_column_pandas_labels=["monotonic_column"],
                data_column_snowflake_quoted_identifiers=data_column_snowflake_quoted_identifiers,
                index_column_pandas_labels=new_modin_frame.index_column_pandas_labels,
                index_column_snowflake_quoted_identifiers=new_modin_frame.index_column_snowflake_quoted_identifiers,
                data_column_types=None,
                index_column_types=None,
            )
        )
        # use agg all to handle empty case
        return qc.agg(func="all", args=(), kwargs={}, axis=0)

    def _add_columns_for_monotonicity_checks(
        self, col_to_check: str, columns_to_add: Optional[str] = None
    ) -> tuple["SnowflakeQueryCompiler", Optional[str], Optional[str]]:
        """
        Adds columns that check for monotonicity (increasing or decreasing) in the
        specified column.

        Parameters
        ----------
        col_to_check : str
            The Snowflake quoted identifier for the column whose monotonicity to check.
        columns_to_add : str, optional
            Whether to add all columns, and if not, which columns to add.

        Returns
        -------
        SnowflakeQueryCompiler, str, str
            A SnowflakeQueryCompiler backed by the InternalFrame with the monotonicity columns,
            and the Snowflake quoted identifiers for the monotonically increasing and monotonically
            decreasing columns (in that order).
        """
        self._raise_not_implemented_error_for_timedelta()

        assert columns_to_add in [
            None,
            "increasing",
            "decreasing",
        ], "Invalid value passed to function"
        modin_frame = self._modin_frame
        modin_frame = modin_frame.ensure_row_position_column()
        row_position_column = modin_frame.row_position_snowflake_quoted_identifier
        monotonic_decreasing_snowflake_quoted_id = None
        monotonic_increasing_snowflake_quoted_id = None
        modin_frame = modin_frame.append_column(
            "_index_lag_col",
            lag(col_to_check).over(Window.order_by(row_position_column)),
        )
        lag_col_snowflake_quoted_id = (
            modin_frame.data_column_snowflake_quoted_identifiers[-1]
        )
        if columns_to_add in [None, "decreasing"]:
            modin_frame = modin_frame.append_column(
                "_is_monotonic_decreasing",
                iff(
                    count("*").over() <= 1,
                    pandas_lit(True),
                    coalesce(
                        min_(
                            col(col_to_check) <= col(lag_col_snowflake_quoted_id)
                        ).over(),
                        pandas_lit(False),
                    ),
                ),
            )
            monotonic_decreasing_snowflake_quoted_id = (
                modin_frame.data_column_snowflake_quoted_identifiers[-1]
            )
        if columns_to_add in [None, "increasing"]:
            modin_frame = modin_frame.append_column(
                "_is_monotonic_increasing",
                iff(
                    count("*").over() <= 1,
                    pandas_lit(True),
                    coalesce(
                        min_(
                            col(col_to_check) >= col(lag_col_snowflake_quoted_id)
                        ).over(),
                        pandas_lit(False),
                    ),
                ),
            )
            monotonic_increasing_snowflake_quoted_id = (
                modin_frame.data_column_snowflake_quoted_identifiers[-1]
            )
        data_column_pandas_labels = modin_frame.data_column_pandas_labels
        data_column_snowflake_quoted_identifiers = (
            modin_frame.data_column_snowflake_quoted_identifiers
        )
        data_column_pandas_labels.remove("_index_lag_col")
        data_column_snowflake_quoted_identifiers.remove(lag_col_snowflake_quoted_id)
        modin_frame = InternalFrame.create(
            ordered_dataframe=modin_frame.ordered_dataframe,
            data_column_pandas_index_names=modin_frame.data_column_pandas_index_names,
            data_column_pandas_labels=data_column_pandas_labels,
            data_column_snowflake_quoted_identifiers=data_column_snowflake_quoted_identifiers,
            index_column_pandas_labels=modin_frame.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=modin_frame.index_column_snowflake_quoted_identifiers,
            data_column_types=None,
            index_column_types=None,
        )
        return (
            SnowflakeQueryCompiler(modin_frame),
            monotonic_increasing_snowflake_quoted_id,
            monotonic_decreasing_snowflake_quoted_id,
        )

    def _reindex_axis_0(
        self,
        labels: Union[pandas.Index, "pd.Index", list[Any], "SnowflakeQueryCompiler"],
        **kwargs: dict[str, Any],
    ) -> "SnowflakeQueryCompiler":
        """
        Align QueryCompiler data with a new index.

        Parameters
        ----------
        labels : list-like, SnowflakeQueryCompiler
            Index-labels to align with.
        method : {None, "backfill"/"bfill", "pad"/"ffill", "nearest"}
            Method to use for filling holes in reindexed frame.
        fill_value : scalar
            Value to use for missing values in the resulted frame.
        limit : int
        tolerance : int
        **kwargs : dict
            Serves the compatibility purpose. Does not affect the result.

        Returns
        -------
        SnowflakeQueryCompiler
            QueryCompiler with aligned axis.
        """
        self._raise_not_implemented_error_for_timedelta()

        if isinstance(labels, SnowflakeQueryCompiler):
            new_index_qc = labels
        else:
            if isinstance(labels, native_pd.Index):
                labels = pd.Index(labels)
            if isinstance(labels, pd.Index):
                new_index_qc = labels.to_series()._query_compiler
            else:
                new_index_qc = pd.Series(labels)._query_compiler

        new_index_modin_frame = new_index_qc._modin_frame
        modin_frame = self._modin_frame
        method = kwargs.get("method", None)
        fill_value = kwargs.get("fill_value", np.nan)  # type: ignore[arg-type]
        limit = kwargs.get("limit", None)
        _filter_column_snowflake_quoted_id = None
        is_index = kwargs.get("_is_index", False)
        if is_index:
            modin_frame = modin_frame.ensure_row_position_column()
            row_position_column = modin_frame.row_position_snowflake_quoted_identifier
            modin_frame = modin_frame.append_column("indices", col(row_position_column))
            # We will also add columns to check for monotonicity so that we can throw a similar error as native pandas
            # does for monotonicity. We do this for index objects but not DataFrame's or Series as Index.reindex returns
            # a NumPy array of indices - which requires eager materialization, so we can just materialize the monotonicity
            # check at the same time, and throw the appropriate error.
            new_qc = SnowflakeQueryCompiler(modin_frame)
            index_col_snowflake_quoted_id = (
                modin_frame.index_column_snowflake_quoted_identifiers[0]
            )
            (
                new_qc,
                monotonic_increasing_snowflake_quoted_id,
                monotonic_decreasing_snowflake_quoted_id,
            ) = new_qc._add_columns_for_monotonicity_checks(
                index_col_snowflake_quoted_id
            )
            modin_frame = new_qc._modin_frame
        if fill_value is not np.nan or method:
            # If we are filling values, reindex ignores NaN values that
            # were previously present in the DataFrame before reindexing.
            # In order to differentiate between pre-existing NaN values,
            # and new NaN values caused by new index values that are not
            # present, we can attach a boolean column of all `True`'s to
            # self's modin_frame. After the left join with the new index
            # rows that were present in self will have a True value, while
            # rows that were not present in self will have a NA value. We can
            # filter by which rows have an NA value for the dummy column to determine
            # between pre-existing NaN's, and NaN's that were introduced because of new
            # values in the index that are not present in the old index. If a row
            # has a True value for the dummy column, any NaN's in it should be ignored
            # as it is a pre-existing NaN value that we **should not** fill.
            modin_frame = modin_frame.append_column(
                "dummy_reindex_column_for_fill", pandas_lit(True)
            )
            _filter_column_snowflake_quoted_id = (
                modin_frame.data_column_snowflake_quoted_identifiers[-1]
            )
        # convert index frame to variant type so it can be joined with a frame of differing type
        new_index_modin_frame = convert_index_type_to_variant(new_index_modin_frame)
        result_frame, result_frame_column_mapper = join_utils.join(
            new_index_modin_frame,
            modin_frame,
            how="left",
            left_on=new_index_modin_frame.data_column_snowflake_quoted_identifiers,
            right_on=modin_frame.index_column_snowflake_quoted_identifiers,
        )
        data_column_pandas_labels = modin_frame.data_column_pandas_labels
        data_column_snowflake_quoted_identifiers = (
            result_frame_column_mapper.map_right_quoted_identifiers(
                modin_frame.data_column_snowflake_quoted_identifiers
            )
        )
        if is_index:
            # We want to remove the monotonic_increasing and monotonic_decreasing columns so that they will not be filled here.
            # We kept them for the join, since the join projects out the active columns, so if they were not present in the
            # InternalFrame's data_column_pandas_labels and data_column_snowflake_quoted_identifiers, they would be filtered out.
            (
                row_position_column,
                monotonic_decreasing_snowflake_quoted_id,
                monotonic_increasing_snowflake_quoted_id,
            ) = result_frame_column_mapper.map_right_quoted_identifiers(
                [
                    row_position_column,
                    monotonic_decreasing_snowflake_quoted_id,
                    monotonic_increasing_snowflake_quoted_id,
                ]
            )
            data_column_pandas_labels.remove("_is_monotonic_decreasing")
            data_column_pandas_labels.remove("_is_monotonic_increasing")
            data_column_snowflake_quoted_identifiers.remove(
                monotonic_decreasing_snowflake_quoted_id
            )
            data_column_snowflake_quoted_identifiers.remove(
                monotonic_increasing_snowflake_quoted_id
            )
        new_modin_frame = InternalFrame.create(
            ordered_dataframe=result_frame.ordered_dataframe,
            data_column_pandas_labels=data_column_pandas_labels,
            data_column_snowflake_quoted_identifiers=data_column_snowflake_quoted_identifiers,
            data_column_pandas_index_names=modin_frame.data_column_pandas_index_names,
            index_column_pandas_labels=new_index_modin_frame.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=result_frame_column_mapper.map_left_quoted_identifiers(
                new_index_modin_frame.data_column_snowflake_quoted_identifiers
            ),
            data_column_types=None,
            index_column_types=None,
        )
        new_qc = SnowflakeQueryCompiler(new_modin_frame)
        if method or fill_value is not np.nan:
            new_filter_column_snowflake_quoted_id = (
                result_frame_column_mapper.map_right_quoted_identifiers(
                    [_filter_column_snowflake_quoted_id]
                )[0]
            )
            (
                new_modin_frame,
                mapper,
            ) = new_modin_frame.update_snowflake_quoted_identifiers_with_expressions(
                {
                    new_filter_column_snowflake_quoted_id: coalesce(
                        col(new_filter_column_snowflake_quoted_id),
                        pandas_lit(False),
                    )
                }
            )
            new_filter_column_snowflake_quoted_id = mapper[
                new_filter_column_snowflake_quoted_id
            ]
            if method not in ["nearest", None]:
                new_qc = SnowflakeQueryCompiler(
                    new_modin_frame.ensure_row_position_column()
                )
                ordering_column = (
                    new_qc._modin_frame.row_position_snowflake_quoted_identifier
                )
                new_qc = new_qc.sort_rows_by_column_values(
                    columns=new_modin_frame.index_column_pandas_labels,
                    ascending=[True],
                    kind="stable",
                    na_position="last",
                    ignore_index=False,
                )
                new_qc = new_qc._fillna_with_masking(
                    self_is_series=False,
                    method=method,
                    limit=limit,  # type: ignore[arg-type]
                    axis=0,
                    row_mask_snowflake_quoted_identifier=new_filter_column_snowflake_quoted_id,
                )
                new_ordered_frame = new_qc._modin_frame.ordered_dataframe.sort(
                    OrderingColumn(snowflake_quoted_identifier=ordering_column)
                )
                new_ordered_frame.row_position_snowflake_quoted_identifier = (
                    ordering_column
                )
                new_qc = SnowflakeQueryCompiler(
                    InternalFrame.create(
                        ordered_dataframe=new_ordered_frame,
                        data_column_pandas_labels=new_qc._modin_frame.data_column_pandas_labels[
                            :-1
                        ],
                        data_column_snowflake_quoted_identifiers=new_qc._modin_frame.data_column_snowflake_quoted_identifiers[
                            :-1
                        ],
                        data_column_pandas_index_names=new_qc._modin_frame.data_column_pandas_index_names,
                        index_column_pandas_labels=new_qc._modin_frame.index_column_pandas_labels,
                        index_column_snowflake_quoted_identifiers=new_qc._modin_frame.index_column_snowflake_quoted_identifiers,
                        data_column_types=None,
                        index_column_types=None,
                    )
                )
            if fill_value is not np.nan:
                new_qc = new_qc._fillna_with_masking(
                    self_is_series=False,
                    value=fill_value,
                    axis=0,
                    row_mask_snowflake_quoted_identifier=new_filter_column_snowflake_quoted_id,
                )
                if method in ["nearest", None]:
                    # In this case, we haven't removed the dummy column that tells us which NA values
                    # should not be replaced.
                    new_qc = SnowflakeQueryCompiler(
                        InternalFrame.create(
                            ordered_dataframe=new_qc._modin_frame.ordered_dataframe,
                            data_column_pandas_labels=new_qc._modin_frame.data_column_pandas_labels[
                                :-1
                            ],
                            data_column_snowflake_quoted_identifiers=new_qc._modin_frame.data_column_snowflake_quoted_identifiers[
                                :-1
                            ],
                            data_column_pandas_index_names=new_qc._modin_frame.data_column_pandas_index_names,
                            index_column_pandas_labels=new_qc._modin_frame.index_column_pandas_labels,
                            index_column_snowflake_quoted_identifiers=new_qc._modin_frame.index_column_snowflake_quoted_identifiers,
                            data_column_types=None,
                            index_column_types=None,
                        )
                    )
        if is_index:
            modin_frame = new_qc._modin_frame
            # We need to get the new quoted identifier after filling happens.
            row_position_column = modin_frame.data_column_snowflake_quoted_identifiers[
                modin_frame.data_column_pandas_labels.index("indices")
            ]
            (
                modin_frame,
                mapper,
            ) = modin_frame.update_snowflake_quoted_identifiers_with_expressions(
                {row_position_column: coalesce(row_position_column, pandas_lit(-1))}
            )
            row_position_column = mapper.get(row_position_column, row_position_column)
            # Remove row_position_column
            new_qc = SnowflakeQueryCompiler(
                InternalFrame.create(
                    ordered_dataframe=modin_frame.ordered_dataframe,
                    data_column_pandas_labels=modin_frame.data_column_pandas_labels[
                        :-1
                    ],
                    data_column_snowflake_quoted_identifiers=modin_frame.data_column_snowflake_quoted_identifiers[
                        :-1
                    ],
                    data_column_pandas_index_names=modin_frame.data_column_pandas_index_names,
                    index_column_pandas_labels=modin_frame.index_column_pandas_labels,
                    index_column_snowflake_quoted_identifiers=modin_frame.index_column_snowflake_quoted_identifiers,
                    data_column_types=None,
                    index_column_types=None,
                )
            )
            materialized_frame = new_qc._modin_frame.ordered_dataframe.select(
                [
                    row_position_column,
                    monotonic_decreasing_snowflake_quoted_id,
                    monotonic_increasing_snowflake_quoted_id,
                ]
            ).to_pandas()
            monotonic_decreasing = materialized_frame.iloc[:, 1]
            monotonic_increasing = materialized_frame.iloc[:, -1]
            any_overlap = not monotonic_decreasing.isna().all()
            # If there is no overlap between the target and source indexes, the result_frame will have NA values for every row in the monotonic columns.
            # If this is the case, we shouldn't falsely error out.
            if (
                method is not None
                and not (monotonic_decreasing.all() or monotonic_increasing.all())
                and any_overlap
            ):
                raise ValueError("index must be monotonic increasing or decreasing")
            if limit is not None and method is not None:
                labels_idx = native_pd.Index(labels)
                if (
                    not (
                        monotonic_increasing.all()
                        and labels_idx.is_monotonic_increasing
                    )
                    and any_overlap
                ):
                    method_str = {"bfill": "backfill", "ffill": "pad"}.get(
                        method, method  # type: ignore[call-overload]
                    )
                    raise ValueError(
                        f"limit argument for '{method_str}' method only well-defined if index and target are monotonic"
                    )
            return new_qc, materialized_frame.iloc[:, 0].values  # type: ignore[return-value]
        return new_qc

    def _reindex_axis_1(
        self,
        labels: Union[pandas.Index, list[Any]],
        **kwargs: dict[str, Any],
    ) -> "SnowflakeQueryCompiler":
        """
        Align QueryCompiler data with a new column.

        Parameters
        ----------
        labels : list-like
            Index-labels to align with.
        method : {None, "backfill"/"bfill", "pad"/"ffill", "nearest"}
            Method to use for filling holes in reindexed frame.
        fill_value : scalar
            Value to use for missing values in the resulted frame.
        limit : int
        tolerance : int
        **kwargs : dict
            Serves the compatibility purpose. Does not affect the result.

        Returns
        -------
        SnowflakeQueryCompiler
            QueryCompiler with aligned axis.
        """
        self._raise_not_implemented_error_for_timedelta()

        method = kwargs.get("method", None)
        level = kwargs.get("level", None)
        limit = kwargs.get("limit", None)
        tolerance = kwargs.get("tolerance", None)
        fill_value = kwargs.get("fill_value", np.nan)  # type: ignore[arg-type]
        self.columns.reindex(labels, method, level, limit, tolerance)
        data_column_pandas_labels = []
        data_column_snowflake_quoted_identifiers = []
        modin_frame = self._modin_frame
        for label in labels:
            data_column_pandas_labels += [label]
            if label in self._modin_frame.data_column_pandas_labels:
                snowflake_quoted_ids = list(
                    modin_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                        [label], include_index=False
                    )[0]
                )
                data_column_snowflake_quoted_identifiers += snowflake_quoted_ids
                if len(snowflake_quoted_ids) > 1:
                    data_column_pandas_labels += [label] * (
                        len(snowflake_quoted_ids) - 1
                    )
            else:
                modin_frame = modin_frame.append_column(label, pandas_lit(np.nan))
                data_column_snowflake_quoted_identifiers += [
                    modin_frame.data_column_snowflake_quoted_identifiers[-1]
                ]
        new_modin_frame = InternalFrame.create(
            ordered_dataframe=modin_frame.ordered_dataframe,
            data_column_pandas_labels=data_column_pandas_labels,
            data_column_snowflake_quoted_identifiers=data_column_snowflake_quoted_identifiers,
            data_column_pandas_index_names=self._modin_frame.data_column_pandas_index_names,
            index_column_pandas_labels=self._modin_frame.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=self._modin_frame.index_column_snowflake_quoted_identifiers,
            data_column_types=None,
            index_column_types=None,
        )
        new_qc = SnowflakeQueryCompiler(new_modin_frame)
        ordered_columns = sorted(data_column_pandas_labels)
        columns_to_ignore = [
            c in self._modin_frame.data_column_pandas_labels for c in ordered_columns
        ]
        if method not in ["nearest", None]:
            new_qc = new_qc.take_2d_labels(
                index=slice(None), columns=ordered_columns
            )._fillna_with_masking(
                method=method, limit=limit, columns_mask=columns_to_ignore, self_is_series=False  # type: ignore[arg-type]
            )
        if fill_value is not np.nan:
            new_qc = new_qc.take_2d_labels(
                index=slice(None), columns=ordered_columns
            )._fillna_with_masking(
                value=fill_value,
                columns_mask=columns_to_ignore,
                self_is_series=False,
            )
        if method not in ["nearest", None] or fill_value is not np.nan:
            # We only need to reorder the columns if we sorted them above for filling.
            new_qc = new_qc.take_2d_labels(
                index=slice(None), columns=data_column_pandas_labels
            )
        return new_qc

    def _parse_names_arguments_from_reset_index(
        self,
        names: IndexLabel,
        levels_to_be_reset: list[int],
        index_column_pandas_labels_moved: list[Hashable],
    ) -> list[Hashable]:
        """
        Returns a list of pandas labels from ``names`` argument in ``reset_index`` method.
        The result will be used as pandas labels for columns moved from index columns to data
        columns after ``reset_index`` call.

        Args:
            names: ``names`` argument from ``reset_index`` method
            levels_to_be_reset: A list of integers representing index column levels to be reset.
                It should be returned from ``parse_levels_to_integer_levels`` as
                parsed ``level`` arguments.
            index_column_pandas_labels_moved: a list of current pandas labels moved from index
                columns to data columns. It is only used when names is ``None``.
        """
        if names:
            # validate names
            if isinstance(names, (str, int)):
                names = [names]
            if not isinstance(names, list):
                # Same error message as native pandas.
                raise ValueError("Index names must be str or 1-dimensional list")
            # only keep names corresponding to index columns to be moved to data columns
            # Therefore, if len(names) is greater than number of index columns, additional
            # values are simply ignored; if len(names) is less than number of index columns
            # an IndexError is raised, which are the same as native pandas
            return [
                names[idx]
                for idx in range(self._modin_frame.num_index_columns)
                if idx in levels_to_be_reset
            ]
        else:
            # Replace None with values:
            # 1. Use "index" if no column exists with same name and index is not multi-index.
            # 2. Use "level_{i}' where i is level on index column (starts with 0).
            # Also check the docstring of fill_none_in_index_labels
            return fill_none_in_index_labels(
                index_column_pandas_labels_moved,
                existing_labels=index_column_pandas_labels_moved
                + self._modin_frame.data_column_pandas_labels,
            )

    def _check_duplicates_in_reset_index(
        self, allow_duplicates: bool, index_column_pandas_labels_moved: list[Hashable]
    ) -> None:
        """
        Checks whether pandas labels moved from index columns to data columns have duplicates
        with existing pandas labels of data columns in ``reset_index`` method.
        Args:
            allow_duplicates: If True, check duplicates.
            index_column_pandas_labels_moved: a list of current pandas labels moved from index
                columns to data columns.

        Raises:
            ValueError if there is a conflict.
        """
        if not allow_duplicates:
            pandas_labels_set = set(self._modin_frame.data_column_pandas_labels)
            for pandas_label in index_column_pandas_labels_moved:
                if pandas_label in pandas_labels_set:
                    # Same error message as native pandas.
                    raise ValueError(f"cannot insert {pandas_label}, already exists")
                pandas_labels_set.add(pandas_label)

    def reset_index(
        self,
        level: IndexLabel = None,
        drop: bool = False,
        col_level: Hashable = 0,
        col_fill: Hashable = "",
        allow_duplicates: bool = False,
        names: IndexLabel = None,
    ) -> "SnowflakeQueryCompiler":
        """
        Reset the index, or a level of it.
        Args:
            drop: Whether to drop the reset index or insert it at the beginning of the frame.
            level : Level to remove from index. Removes all levels by default.
            col_level : If the columns have multiple levels, determines which level the labels are inserted into.
            col_fill : If the columns have multiple levels, determines how the other levels are named.
            allow_duplicates: Allow duplicate column lables to be created.
            names: Using the given string, rename the DataFrame column which contains the index data.
                Must be int, str or 1-dimensional list. If the DataFrame has a MultiIndex, this has to be a list or
                tuple with length equal to the number of levels.
        Returns:
            A new SnowflakeQueryCompiler instance with updated index.
        """
        if allow_duplicates is no_default:
            allow_duplicates = False
        # These levels will be moved from index columns to data columns
        levels_to_be_reset = self._modin_frame.parse_levels_to_integer_levels(
            level, allow_duplicates=False
        )

        # index_columns_pandas_labels_moved contains pandas labels moved from index columns
        # to data columns
        # index_columns_pandas_labels_remained contains pandas labels remained in index columns
        # We need to iterate over original index_column_pandas_labels again to make the order
        # of labels in index_columns_pandas_labels_moved consistent with the order in
        # original index_column_pandas_labels. This is to align with pandas.
        # Meanwhile, we extract index_column_snowflake_quoted_identifiers_remained and
        # index_column_snowflake_quoted_identifiers_moved for future use.
        (
            index_column_pandas_labels_moved,
            index_column_snowflake_quoted_identifiers_moved,
            index_column_types_moved,
            index_column_pandas_labels_remained,
            index_column_snowflake_quoted_identifiers_remained,
            index_column_types_remained,
        ) = self._modin_frame.get_snowflake_identifiers_and_pandas_labels_from_levels(
            levels_to_be_reset
        )
        ordered_dataframe = self._modin_frame.ordered_dataframe

        # if all index columns are reset, assign a default index with row position column
        if len(index_column_pandas_labels_remained) == 0:
            index_column_snowflake_quoted_identifier = (
                ordered_dataframe.generate_snowflake_quoted_identifiers(
                    pandas_labels=[INDEX_LABEL],
                    wrap_double_underscore=True,
                )[0]
            )
            # duplicate the row position column as the new index column
            ordered_dataframe = ordered_dataframe.ensure_row_position_column()
            ordered_dataframe = append_columns(
                ordered_dataframe,
                index_column_snowflake_quoted_identifier,
                col(ordered_dataframe.row_position_snowflake_quoted_identifier),
            )
            index_column_pandas_labels_remained = [
                None
            ]  # by default index label is None
            index_column_snowflake_quoted_identifiers_remained = [
                index_column_snowflake_quoted_identifier
            ]
            index_column_types_remained = [None]

        # Do not drop existing index columns and move them to data columns.
        if not drop:
            # Get new pandas labels based on names arguments or existing index columns.
            new_index_column_pandas_labels_moved = (
                self._parse_names_arguments_from_reset_index(
                    names, levels_to_be_reset, index_column_pandas_labels_moved
                )
            )

            if (
                new_index_column_pandas_labels_moved
                and self._modin_frame.is_multiindex(axis=1)
            ):
                # If data column is multiindex, try to re-construct the index pandas label
                # to align with the same number of levels as data column labels by applying filling rules.
                num_levels = self._modin_frame.num_index_levels(axis=1)
                int_col_level = self._modin_frame.parse_levels_to_integer_levels(
                    [col_level], allow_duplicates=False, axis=1
                )[0]

                new_index_column_pandas_labels_moved_with_filling = []
                for index_label in new_index_column_pandas_labels_moved:
                    fill_value = col_fill
                    index_label_components = (
                        list(index_label)
                        if isinstance(index_label, tuple)
                        else [index_label]
                    )
                    if col_fill is None:
                        if len(index_label_components) not in (1, num_levels):
                            # this is consistent with pandas, it requires the length of the label to either 1 or
                            # same as num_levels
                            raise ValueError(
                                "col_fill=None is incompatible "
                                f"with incomplete column name {index_label}"
                            )
                        # According to pandas doc, if fill value is None, it repeats the index name.
                        # Note that Snowpark pandas behavior is different compare with current pandas,
                        # current pandas set the filling value with the first index name it finds, and
                        # since it handles the index in reverse order, it fills with the last index value.
                        # For example, if the index names are ['a', 'b'], 'b' is always used as filling
                        # value even when fill the index 'a'. This is because the implementation does an inplace
                        # update of col_fill, which seems an implementation bug, and not consistent with
                        # the doc.
                        # With Snowpark pandas, we provide the behavior same as the document that repeats
                        # the index name for the index to fill.
                        fill_value = index_label_components[0]

                    filled_index_label = fill_missing_levels_for_pandas_label(
                        index_label, num_levels, int_col_level, fill_value
                    )
                    new_index_column_pandas_labels_moved_with_filling.append(
                        filled_index_label
                    )

                new_index_column_pandas_labels_moved = (
                    new_index_column_pandas_labels_moved_with_filling
                )

            # Check for duplicates and raise error if there is a conflict.
            self._check_duplicates_in_reset_index(
                allow_duplicates, new_index_column_pandas_labels_moved
            )

            # Move existing index columns to data columns.
            data_column_pandas_labels = (
                new_index_column_pandas_labels_moved
                + self._modin_frame.data_column_pandas_labels
            )
            data_column_snowflake_quoted_identifiers = (
                index_column_snowflake_quoted_identifiers_moved
                + self._modin_frame.data_column_snowflake_quoted_identifiers
            )

            data_column_types = (
                index_column_types_moved
                + self._modin_frame.cached_data_column_snowpark_pandas_types
            )

        else:
            data_column_pandas_labels = self._modin_frame.data_column_pandas_labels
            data_column_snowflake_quoted_identifiers = (
                self._modin_frame.data_column_snowflake_quoted_identifiers
            )
            data_column_types = (
                self._modin_frame.cached_data_column_snowpark_pandas_types
            )

        internal_frame = InternalFrame.create(
            ordered_dataframe=ordered_dataframe,
            data_column_pandas_labels=data_column_pandas_labels,
            data_column_snowflake_quoted_identifiers=data_column_snowflake_quoted_identifiers,
            data_column_pandas_index_names=self._modin_frame.data_column_pandas_index_names,
            data_column_types=data_column_types,
            index_column_pandas_labels=index_column_pandas_labels_remained,
            index_column_snowflake_quoted_identifiers=index_column_snowflake_quoted_identifiers_remained,
            index_column_types=index_column_types_remained,
        )

        return SnowflakeQueryCompiler(internal_frame)

    # TODO: Eliminate from Modin QC layer and call `first_last_valid_index` directly from frontend
    def first_valid_index(self) -> Union[Scalar, tuple[Scalar]]:
        """
        Return index for first non-NA value or None, if no non-NA value is found.

        Returns:
            scalar or None, Tuple of scalars if MultiIndex
        """
        return self.first_last_valid_index(ValidIndex.FIRST)

    # TODO: Eliminate from Modin QC layer and call `first_last_valid_index` directly from frontend
    def last_valid_index(self) -> Union[Scalar, tuple[Scalar]]:
        """
        Return index for last non-NA value or None, if no non-NA value is found.

        Returns:
            scalar or None, Tuple of scalars if MultiIndex
        """
        return self.first_last_valid_index(ValidIndex.LAST)

    def first_last_valid_index(
        self,
        first_or_last: ValidIndex,
    ) -> Union[Scalar, tuple[Scalar]]:
        """
        Helper function to get first or last valid index.

        Parameters:
            first_or_last: Enum specifying which valid index to return.
                Can be either ValidIndex.FIRST or ValidIndex.LAST.

        Returns:
            scalar or None, Tuple of scalars if MultiIndex
        """
        # Results in a Series with boolean values. If any value in the Series is True,
        # all values of the corresponding row of the input df exist
        qc = self.notna().any(axis=1, bool_only=False, skipna=True)
        # Filter for True values and get index based on first_or_last
        valid_index_values = get_valid_index_values(
            frame=qc._modin_frame, first_or_last=first_or_last
        )

        if valid_index_values:
            return convert_snowpark_row_to_pandas_index(
                valid_index_values=valid_index_values,
                index_dtypes=self.index_dtypes,
            )
        return None

    def sort_index(
        self,
        *,
        axis: int,
        level: Optional[list[Union[str, int]]],
        ascending: Union[bool, list[bool]],
        inplace: bool = False,
        kind: SortKind,
        na_position: NaPosition,
        sort_remaining: bool,
        ignore_index: bool,
        key: Optional[IndexKeyFunc] = None,
        include_indexer: bool = False,
    ) -> "SnowflakeQueryCompiler":
        """
        Sort object by labels (along an axis).

        Args:
            axis: The axis along which to sort.
            level: If not None, sort on values in specified index level(s).
            ascending: A list of bools to represent ascending vs descending sort. Defaults to True.
                When the index is a MultiIndex the sort direction can be controlled for each level individually.
            inplace: Whether or not the sort occurs in-place. This argument is ignored and only provided
                for compatibility with Modin.
            kind: Choice of sorting algorithm. Perform stable sort if 'stable'. Defaults to unstable sort.
                Snowpark pandas ignores choice of sorting algorithm except 'stable'.
            na_position: Puts NaNs at the beginning if 'first'; 'last' puts NaNs at the end. Defaults to 'last'
            sort_remaining: If True and sorting by level and index is multilevel, then sort by other levels
                too (in order) after sorting by specified level.
            ignore_index: If True, existing index is ignored and new index is generated which is a gap free
                sequence from 0 to n-1. Defaults to False.
            key: If not None, apply the key function to the index values before sorting. This is similar to
                the key argument in the builtin sorted() function, with the notable difference that this key
                function should be vectorized. It should expect an Index and return an Index of the same shape.
                Apply the key function to the index values before sorting.
            include_indexer: If True, add a data column with the original row numbers in the same order as
                the index, i.e., add an indexer column. This is used with Index.sort_values.

        Returns:
            A new SnowflakeQueryCompiler instance after applying the sort.

        Examples:
        >>> s = pd.Series(['a', 'b', 'c', 'd'], index=[3, 2, 1, np.nan])
        >>> s.sort_index()
        1.0    c
        2.0    b
        3.0    a
        NaN    d
        dtype: object
        >>> s.sort_index(ignore_index=True)
        0    c
        1    b
        2    a
        3    d
        dtype: object
        >>> s.sort_index(ascending=False, na_position="first")
        NaN    d
        3.0    a
        2.0    b
        1.0    c
        dtype: object
        """
        if axis in (1, "index"):
            ErrorMessage.not_implemented(
                "sort_index is not supported yet on axis=1 in Snowpark pandas."
            )
        if key:
            ErrorMessage.not_implemented(
                "Snowpark pandas sort_index API doesn't yet support 'key' parameter"
            )

        if self._modin_frame.is_multiindex() or level is not None:
            ErrorMessage.not_implemented(
                "sort_index() with multi index is not supported yet in Snowpark pandas."
            )

        return self.sort_rows_by_column_values(
            columns=self.get_index_names(),
            ascending=ascending if isinstance(ascending, list) else [ascending],
            kind=kind,
            na_position=na_position,
            ignore_index=ignore_index,
            key=key,
            include_indexer=include_indexer,
        )

    def sort_columns_by_row_values(
        self, rows: IndexLabel, ascending: bool = True, **kwargs: Any
    ) -> None:
        """
        Reorder the columns based on the lexicographic order of the given rows.

        Args:
            rows : label or list of labels
                The row or rows to sort by.
            ascending : bool, default: True
                Sort in ascending order (True) or descending order (False).
            **kwargs : dict
                Serves the compatibility purpose. Does not affect the result.

        Returns:
            New QueryCompiler that contains result of the sort.
        """
        ErrorMessage.not_implemented(
            "Snowpark pandas sort_values API doesn't yet support axis == 1"
        )

    def sort_rows_by_column_values(
        self,
        columns: list[Hashable],
        ascending: list[bool],
        kind: SortKind,
        na_position: NaPosition,
        ignore_index: bool,
        key: Optional[IndexKeyFunc] = None,
        include_indexer: bool = False,
        include_index: bool = True,
    ) -> "SnowflakeQueryCompiler":
        """
        Reorder the rows based on the lexicographic order of the given columns.

        Args:
            columns: A list of columns to sort by
            ascending: A list of bools to represent ascending vs descending sort. Defaults to True.
            kind: Choice of sorting algorithm. Perform stable sort if 'stable'. Defaults to unstable sort.
                Snowpark pandas ignores choice of sorting algorithm except 'stable'.
            na_position: Puts NaNs at the beginning if 'first'; 'last' puts NaNs at the end. Defaults to 'last'
            ignore_index: If True, existing index is ignored and new index is generated which is a gap free
                sequence from 0 to n-1. Defaults to False.
            key: Apply the key function to the values before sorting.
            include_indexer: If True, add a data column with the original row numbers in the same order as
                the index, i.e., add an indexer column. This is used with Index.sort_values.
            include_index: If True, include index columns in the sort.

        Returns:
            A new SnowflakeQueryCompiler instance after applying the sort.
        """
        # Check for empty column list, this is a no-op in native pandas.
        # Snowpark dataframe doesn't allow sorting on empty list hence we need this explicit check here.
        if len(columns) == 0:
            return self

        if key:
            ErrorMessage.not_implemented(
                "Snowpark pandas sort_values API doesn't yet support 'key' parameter"
            )

        # In native pandas, 'kind' option is only applied when sorting on a single column or label.
        if len(columns) == 1:
            if kind not in get_args(SortKind):
                # This error message is different from native pandas hence, hence it is kept here instead
                # of moving this to frontend layer.
                raise ValueError(f"sort kind must be 'stable' or None (got '{kind}')")
            # Do not show warning for 'quicksort' as this the default option.
            if kind not in ("stable", "quicksort"):
                logging.warning(
                    f"choice of sort algorithm '{kind}' is ignored. sort kind must be 'stable', 'quicksort', or None"
                )

        matched_identifiers = (
            self._modin_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                columns, include_index
            )
        )

        # Create ordering columns
        na_last = na_position == "last"
        ordering_columns = [
            OrderingColumn(identifiers[0], asc, na_last)
            for identifiers, asc in zip(matched_identifiers, ascending)
        ]

        # We want to provide stable sort even if user provided sort kind is not 'stable'. We are doing this to make
        # ordering deterministic.
        # Snowflake backend sort is unstable. Add row position to ordering columns to make sort stable.
        internal_frame = self._modin_frame.ensure_row_position_column()
        ordered_dataframe = internal_frame.ordered_dataframe.sort(
            *ordering_columns,
            OrderingColumn(internal_frame.row_position_snowflake_quoted_identifier),
        )

        data_column_pandas_labels = internal_frame.data_column_pandas_labels
        data_column_snowflake_quoted_identifiers = (
            internal_frame.data_column_snowflake_quoted_identifiers
        )
        if include_indexer:
            data_column_pandas_labels.append("indexer")
            data_column_snowflake_quoted_identifiers.append(
                internal_frame.row_position_snowflake_quoted_identifier
            )
        data_column_types = [
            internal_frame.snowflake_quoted_identifier_to_snowpark_pandas_type.get(
                id, None
            )
            for id in data_column_snowflake_quoted_identifiers
        ]
        sorted_frame = InternalFrame.create(
            ordered_dataframe=ordered_dataframe,
            data_column_pandas_labels=data_column_pandas_labels,
            data_column_snowflake_quoted_identifiers=data_column_snowflake_quoted_identifiers,
            data_column_pandas_index_names=internal_frame.data_column_pandas_index_names,
            index_column_pandas_labels=internal_frame.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=internal_frame.index_column_snowflake_quoted_identifiers,
            data_column_types=data_column_types,
            index_column_types=internal_frame.cached_index_column_snowpark_pandas_types,
        )
        sorted_qc = SnowflakeQueryCompiler(sorted_frame)

        if ignore_index:
            sorted_qc = sorted_qc.reset_index(drop=True)
        return sorted_qc

    def validate_groupby(
        self,
        by: Any,
        axis: int,
        level: Optional[IndexLabel],
    ) -> None:
        """
        This function only performs validation for groupby that need access to the information
        of internal frame.

        Args:
            by: mapping, SnowSeries, callable, label, pd.Grouper, list of such. Used to determine the groups for the groupby.
            axis: 0 (index), 1 (columns)
            level: Optional[IndexLabel]. The IndexLabel can be int, level name, or sequence of such.
                    If the axis is a MultiIndex (hierarchical), group by a particular level or levels.
        Raises:
            ValueError if no by item is passed
            KeyError if a hashable label in by (groupby items) can not be found in the current dataframe
            ValueError if more than one column can be found for the groupby item
        """
        validate_groupby_columns(self, by, axis, level)

    def groupby_ngroups(
        self,
        by: Any,
        axis: int,
        groupby_kwargs: dict[str, Any],
    ) -> int:
        self._raise_not_implemented_error_for_timedelta()

        level = groupby_kwargs.get("level", None)
        dropna = groupby_kwargs.get("dropna", True)

        is_supported = check_is_groupby_supported_by_snowflake(by, level, axis)
        if not is_supported:
            ErrorMessage.not_implemented(
                f"Snowpark pandas GroupBy.ngroups {_GROUPBY_UNSUPPORTED_GROUPING_MESSAGE}"
            )

        query_compiler = get_frame_with_groupby_columns_as_index(
            self, by, level, dropna
        )

        if query_compiler is None:
            ErrorMessage.not_implemented(
                f"Snowpark pandas GroupBy.ngroups {_GROUPBY_UNSUPPORTED_GROUPING_MESSAGE}"
            )

        internal_frame = query_compiler._modin_frame

        return count_rows(
            get_groups_for_ordered_dataframe(
                internal_frame.ordered_dataframe,
                internal_frame.index_column_snowflake_quoted_identifiers,
            )
        )  # pragma: no cover

    def groupby_agg(
        self,
        by: Any,
        agg_func: AggFuncType,
        axis: int,
        groupby_kwargs: dict[str, Any],
        agg_args: Any,
        agg_kwargs: dict[str, Any],
        how: str = "axis_wise",
        numeric_only: bool = False,
        is_series_groupby: bool = False,
        drop: bool = False,
    ) -> "SnowflakeQueryCompiler":
        """
        compute groupby with aggregation functions.
        Note: groupby with categorical data type expands all categories during groupby, for example,
        with a dataframe created with following:
        cat = pd.Categorical([0, 1, 2])
        df = pd.DataFrame({"A": cat, "B": [2, 1, 1], "C": [2, 2, 0]})
            A	B	C
        0	0	2	2
        1	1	1	2
        2	2	1	0
        And df.groupby(['A', 'B']).max() gives the following result:
                C
        A	B
        0	1	NaN
            2	2.0
        1	1	2.0
            2	NaN
        2	1	0.0
            2	NaN
        It creates one group for the cross product of each distinct value of the groupby columns [0, 1, 2] * [1, 2],
        instead of having one group per unique combination of the groupby columns.
        Categorical data type is currently not supported by Snowpark pandas API, such case will not happen.
        TODO (SNOW-895114): Handle Categorical data type in groupby once Categorical DType is supported.

        Args:
            by: mapping, series, callable, label, pd.Grouper, BaseQueryCompiler, list of such.
                Used to determine the groups for the groupby.
            agg_func: callable, str, list or dict. the aggregation function used.
            axis : 0 (index), 1 (columns)
            groupby_kwargs: keyword arguments passed for the groupby. The groupby keywords handled in the
                    function contains:
                    level: int, level name, or sequence of such, default None. If the axis is a MultiIndex(hierarchical),
                           group by a particular level or levels. Do not specify both by and level.
                    sort: bool, default True. Sort group keys. Groupby preserves the order of rows within each group.
                    dropna: bool, default True. If True, and if group keys contain NA values, NA values together with
                        row/column will be dropped. f False, NA values will also be treated as the key in groups.
            agg_args: the arguments passed for the aggregation
            agg_kwargs: keyword arguments passed for the aggregation function.
            how: str. how the aggregation function can be applied.
            numeric_only: bool. whether to drop the non-numeric columns during aggregation.
            is_series_groupby: bool. whether the aggregation is called on SeriesGroupBy or not.
            drop: Modin argument (??)
        Returns:
            SnowflakeQueryCompiler: with a newly constructed internal dataframe
        """
        level = groupby_kwargs.get("level", None)

        if agg_func in ["head", "tail"]:
            # head and tail cannot be run per column - it is run on the
            # whole table at once.
            return self._groupby_head_tail(
                n=agg_kwargs.get("n", 5),
                op_type=agg_func,
                by=by,
                level=level,
                dropna=agg_kwargs.get("dropna", True),
            )
        (
            is_supported,
            unsupported_arguments,
            is_supported_kwargs,
        ) = check_is_aggregation_supported_in_snowflake(agg_func, agg_kwargs, axis)
        if not is_supported:
            raise AttributeError(
                f"'SeriesGroupBy' object has no attribute {repr_aggregate_function(unsupported_arguments, is_supported_kwargs)}"
            )

        sort = groupby_kwargs.get("sort", True)
        as_index = groupby_kwargs.get("as_index", True)
        dropna = groupby_kwargs.get("dropna", True)
        uses_named_aggs = False

        original_index_column_labels = self._modin_frame.index_column_pandas_labels

        query_compiler = get_frame_with_groupby_columns_as_index(
            self, by, level, dropna
        )

        if query_compiler is None or not check_is_groupby_supported_by_snowflake(
            by, level, axis
        ):
            ErrorMessage.not_implemented(
                f"Snowpark pandas GroupBy.aggregate {_GROUPBY_UNSUPPORTED_GROUPING_MESSAGE}"
            )

        by_list = query_compiler._modin_frame.index_column_pandas_labels

        if numeric_only:
            # drop off the non-numeric data columns if the data column is not part of the groupby columns
            query_compiler = drop_non_numeric_data_columns(
                query_compiler,
                pandas_labels_for_columns_to_exclude=by_list,
            )

        internal_frame = query_compiler._modin_frame

        # get a map between the Snowpark pandas column to the aggregation function needs to be applied on the column
        column_to_agg_func = convert_agg_func_arg_to_col_agg_func_map(
            internal_frame,
            agg_func,
            pandas_labels_for_columns_to_exclude_when_agg_on_all=by_list,
        )

        # turn each agg function into an AggFuncInfo named tuple, where is_dummy_agg is set to false;
        # i.e., none of the aggregations here can be dummy.
        def convert_func_to_agg_func_info(
            func: Union[AggFuncType, AggFuncWithLabel]
        ) -> AggFuncInfo:
            nonlocal uses_named_aggs
            if is_named_tuple(func):
                uses_named_aggs = True
                return AggFuncInfo(
                    func=func.func,
                    is_dummy_agg=False,
                    post_agg_pandas_label=func.pandas_label,
                )
            else:
                return AggFuncInfo(
                    func=func, is_dummy_agg=False, post_agg_pandas_label=None
                )

        column_to_agg_func = {
            agg_col: (
                [convert_func_to_agg_func_info(fn) for fn in func]
                if is_list_like(func) and not is_named_tuple(func)
                else convert_func_to_agg_func_info(func)
            )
            for (agg_col, func) in column_to_agg_func.items()
        }

        # get the quoted identifiers for all the by columns. After set_index_from_columns,
        # the index columns of the internal frame are the groupby columns.
        by_snowflake_quoted_identifiers = (
            internal_frame.index_column_snowflake_quoted_identifiers
        )

        # We need to check if `first` or `last` are in the aggregation functions,
        # as we need to ensure a row position column and pass it in as an agg_kwarg
        # if it is (for the min_by/max_by function).
        first_last_present = is_first_last_in_agg_funcs(column_to_agg_func)
        if first_last_present:
            internal_frame = internal_frame.ensure_row_position_column()
            agg_kwargs[
                "_first_last_row_pos_col"
            ] = internal_frame.row_position_snowflake_quoted_identifier
        agg_col_ops, new_data_column_index_names = generate_column_agg_info(
            internal_frame, column_to_agg_func, agg_kwargs, is_series_groupby
        )
        if first_last_present:
            agg_kwargs.pop("_first_last_row_pos_col")
        # the pandas label and quoted identifier generated for each result column
        # after aggregation will be used as new pandas label and quoted identifiers.
        new_data_column_pandas_labels = []
        new_data_column_quoted_identifiers = []
        new_data_column_snowpark_pandas_types = []
        for agg_col_op in agg_col_ops:
            new_data_column_pandas_labels.append(agg_col_op.agg_pandas_label)
            new_data_column_quoted_identifiers.append(
                agg_col_op.agg_snowflake_quoted_identifier
            )
            new_data_column_snowpark_pandas_types.append(
                agg_col_op.data_type
                if isinstance(agg_col_op.data_type, SnowparkPandasType)
                and agg_col_op.snowflake_agg_func.preserves_snowpark_pandas_types
                else None
            )
        # The ordering of the named aggregations is changed by us when we process
        # the agg_kwargs into the func dict (named aggregations on the same
        # column are moved to be contiguous, see groupby.py::aggregate for an
        # example). We need to check if the order of the output columns is correct,
        # and if not, reorder them.
        if uses_named_aggs:
            correct_ordering = list(agg_kwargs.keys())
            if correct_ordering != new_data_column_pandas_labels:
                # In this case, we need to reorder the new_data_column_pandas_labels
                # and the new_data_column_quoted_identifier.
                data_column_label_to_quoted_identifier = list(
                    zip(
                        new_data_column_pandas_labels,
                        new_data_column_quoted_identifiers,
                    )
                )
                (
                    new_data_column_pandas_labels,
                    new_data_column_quoted_identifiers,
                ) = list(
                    zip(
                        *[
                            pair
                            for column_label in correct_ordering
                            for pair in filter(
                                lambda pair: pair[0] == column_label,
                                data_column_label_to_quoted_identifier,
                            )
                        ]
                    )
                )
        if sort:
            # when sort is True, the result is ordered by the groupby keys
            ordering_columns = [
                OrderingColumn(quoted_identifier)
                for quoted_identifier in by_snowflake_quoted_identifiers
            ]
        else:
            # when sort is False, the order is decided by the position of the groupby
            # keys in the original dataframe. In order to recover the order, we retain
            # min(row_position) in the aggregation result.
            internal_frame = internal_frame.ensure_row_position_column()
            row_position_quoted_identifier = (
                internal_frame.row_position_snowflake_quoted_identifier
            )
            row_position_agg_column_op = AggregateColumnOpParameters(
                snowflake_quoted_identifier=row_position_quoted_identifier,
                data_type=internal_frame.get_snowflake_type(
                    row_position_quoted_identifier
                ),
                agg_pandas_label=None,
                agg_snowflake_quoted_identifier=row_position_quoted_identifier,
                snowflake_agg_func=get_snowflake_agg_func("min", agg_kwargs={}, axis=0),
                ordering_columns=internal_frame.ordering_columns,
            )
            agg_col_ops.append(row_position_agg_column_op)
            ordering_columns = [OrderingColumn(row_position_quoted_identifier)]

        ordered_dataframe = internal_frame.ordered_dataframe

        if len(agg_col_ops) == 0:
            # if no columns to aggregate on, return all distinct groups of the dataframe
            # the groupby columns will be used as ordering column in the result
            ordered_dataframe = get_groups_for_ordered_dataframe(
                ordered_dataframe, by_snowflake_quoted_identifiers
            )
        else:
            # get the group by agg result for the data frame
            # the columns of the snowpark dataframe will be groupby columns + aggregation columns
            ordered_dataframe = aggregate_with_ordered_dataframe(
                ordered_dataframe=ordered_dataframe,
                agg_col_ops=agg_col_ops,
                agg_kwargs=agg_kwargs,
                groupby_columns=by_snowflake_quoted_identifiers,
                # index_column_snowflake_quoted_identifier is used for idxmax/idxmin - we use the original index.
                index_column_snowflake_quoted_identifier=self._modin_frame.index_column_snowflake_quoted_identifiers,
            )
        ordered_dataframe = ordered_dataframe.sort(ordering_columns)

        new_index_column_pandas_labels = internal_frame.index_column_pandas_labels
        new_index_column_quoted_identifiers = (
            internal_frame.index_column_snowflake_quoted_identifiers
        )
        drop = False
        if not as_index and not uses_named_aggs:
            # drop off the index columns that are from the original index columns and also the index
            # columns that are from data column with aggregation function applied.
            # For example: with the following dataframe, which has data column ['A', 'B', 'C', 'D', 'E']
            #   A       B       C       D       E
            # 0 foo     one     small   1       2
            # 1	foo     one     large   2   	4
            # 2	foo     two     small   3       5
            # 3	foo     two     small   3       6
            # 4	bar     one     small   5       8
            # 5	bar     two     small   6       9
            # After apply df.groupby(['A', 'B'], as_index=False).agg({"A": min, 'C': max}), the result is following:
            #   B	A	C
            # 0	one	bar	small
            # 1	two	bar	small
            # 2	one	foo	small
            # 3	two	foo	small
            # Where groupby column 'A' is dropped because it is used in aggregation min, but column 'B' is retained
            # because it is originally a data column, and not used in any aggregation.
            new_index_column_pandas_labels_to_keep = []
            new_index_column_quoted_identifiers_to_keep = []
            origin_agg_column_labels = [
                pandas_label for pandas_label, _ in column_to_agg_func.keys()
            ]
            for label, quoted_identifier in zip(
                internal_frame.index_column_pandas_labels,
                internal_frame.index_column_snowflake_quoted_identifiers,
            ):
                if (
                    label not in original_index_column_labels
                    and label not in origin_agg_column_labels
                ):
                    new_index_column_pandas_labels_to_keep.append(label)
                    new_index_column_quoted_identifiers_to_keep.append(
                        quoted_identifier
                    )

            if len(new_index_column_pandas_labels_to_keep) > 0:
                # if there are columns needs to be retained, we reset the index columns to the
                # columns needs to be retained, and call reset_index with drop = False later to
                # keep those column as data columns.
                new_index_column_pandas_labels = new_index_column_pandas_labels_to_keep
                new_index_column_quoted_identifiers = (
                    new_index_column_quoted_identifiers_to_keep
                )
            else:
                # if all index column needs to be dropped, we simply set drop to be True, and
                # reset_index will drop all current index columns.
                drop = True

        query_compiler = SnowflakeQueryCompiler(
            InternalFrame.create(
                ordered_dataframe=ordered_dataframe,
                # original pandas label for data columns are still used as pandas labels
                data_column_pandas_labels=new_data_column_pandas_labels,
                data_column_pandas_index_names=new_data_column_index_names,
                data_column_snowflake_quoted_identifiers=new_data_column_quoted_identifiers,
                index_column_pandas_labels=new_index_column_pandas_labels,
                index_column_snowflake_quoted_identifiers=new_index_column_quoted_identifiers,
                data_column_types=new_data_column_snowpark_pandas_types,
                index_column_types=[
                    internal_frame.snowflake_quoted_identifier_to_snowpark_pandas_type.get(
                        identifier
                    )
                    for identifier in new_index_column_quoted_identifiers
                ],
            )
        )

        return query_compiler if as_index else query_compiler.reset_index(drop=drop)

    def groupby_apply(
        self,
        by: Any,
        agg_func: Callable,
        axis: int,
        groupby_kwargs: dict[str, Any],
        agg_args: Any,
        agg_kwargs: dict[str, Any],
        series_groupby: bool,
        include_groups: bool,
        force_single_group: bool = False,
        force_list_like_to_series: bool = False,
    ) -> "SnowflakeQueryCompiler":
        """
        Group according to `by` and `level`, apply a function to each group, and combine the results.

        Args
        ----
            by:
                The columns or index levels to group by.
            agg_func:
                The function to apply to each group.
            axis:
                The axis along which to form groups.
            groupby_kwargs:
                Keyword arguments for the groupby object, i.e. for the df.groupby() call.
            agg_args:
                Positional arguments to pass to agg_func when applying it to each group.
            agg_kwargs:
                Keyword arguments to pass to agg_func when applying it to each group.
            series_groupby:
                Whether we are performing a SeriesGroupBy.apply() instead of a DataFrameGroupBy.apply()
            include_groups:
                When True, will include grouping keys when calling func in the case that
                they are columns of the DataFrame.
            force_single_group:
                Force single group (empty set of group by labels) useful for DataFrame.apply() with axis=0
            force_list_like_to_series:
                Force the function result to series if it is list-like

        Returns
        -------
            A query compiler with the result.
        """
        self._raise_not_implemented_error_for_timedelta()

        level = groupby_kwargs.get("level", None)
        if not check_is_groupby_supported_by_snowflake(by, level, axis):
            ErrorMessage.not_implemented(
                f"No support for groupby.apply with parameters by={by}, "
                + f"level={level}, and axis={axis}"
            )

        sort = groupby_kwargs.get("sort", True)
        as_index = groupby_kwargs.get("as_index", True)
        dropna = groupby_kwargs.get("dropna", True)
        group_keys = groupby_kwargs.get("group_keys", False)

        if force_single_group:
            query_compiler, by_pandas_labels = self, []
        else:
            (
                query_compiler,
                by_pandas_labels,
            ) = resample_and_extract_groupby_column_pandas_labels(self, by, level)

        _modin_frame = query_compiler._modin_frame

        by_snowflake_quoted_identifiers_list = (
            []
            if force_single_group
            else [
                quoted_identifier
                for entry in _modin_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                    by_pandas_labels
                )
                for quoted_identifier in entry
            ]
        )

        snowflake_type_map = _modin_frame.quoted_identifier_to_snowflake_type()
        input_data_column_positions = [
            i
            for i, identifier in enumerate(
                _modin_frame.data_column_snowflake_quoted_identifiers
            )
            if (
                (
                    # For SeriesGroupBy, this frame may also include some
                    # grouping columns that `func` should not take as input. In
                    # that case, the only column that `func` takes as input is
                    # the last data column, so take just that column.
                    # include_groups has no effect.
                    i
                    == len(_modin_frame.data_column_snowflake_quoted_identifiers) - 1
                )
                if series_groupby
                else (
                    # For DataFrameGroupBy, if include_groups, we apply the
                    # function to all data columns. Otherwise, we exclude
                    # data columns that we are grouping by.
                    include_groups
                    or identifier not in by_snowflake_quoted_identifiers_list
                )
            )
        ]
        input_data_column_identifiers = [
            _modin_frame.data_column_snowflake_quoted_identifiers[i]
            for i in input_data_column_positions
        ]

        # TODO(SNOW-1210489): When type hints show that `agg_func` returns a
        # scalar, we can use a vUDF instead of a vUDTF and we can skip the
        # pivot.
        data_columns_index = _modin_frame.data_columns_index[
            input_data_column_positions
        ]
        is_transform = groupby_kwargs.get("apply_op") == "transform"
        output_schema, udtf = create_udtf_for_groupby_apply(
            agg_func,
            agg_args,
            agg_kwargs,
            data_column_index=data_columns_index,
            index_column_names=_modin_frame.index_column_pandas_labels,
            input_data_column_types=[
                snowflake_type_map[quoted_identifier]
                for quoted_identifier in input_data_column_identifiers
            ],
            input_index_column_types=[
                snowflake_type_map[quoted_identifier]
                for quoted_identifier in _modin_frame.index_column_snowflake_quoted_identifiers
            ],
            session=_modin_frame.ordered_dataframe.session,
            series_groupby=series_groupby,
            by_labels=by_pandas_labels,
            by_types=[]
            if force_single_group
            else [
                snowflake_type_map[quoted_identifier]
                for quoted_identifier in by_snowflake_quoted_identifiers_list
            ],
            existing_identifiers=_modin_frame.ordered_dataframe._dataframe_ref.snowflake_quoted_identifiers,
            force_list_like_to_series=force_list_like_to_series,
            is_transform=is_transform,
            force_single_group=force_single_group,
        )

        new_internal_df = _modin_frame.ensure_row_position_column()

        # drop the rows if any value in groupby key is NaN
        ordered_dataframe = new_internal_df.ordered_dataframe
        if dropna:
            ordered_dataframe = ordered_dataframe.dropna(
                subset=by_snowflake_quoted_identifiers_list
            )

        """
        Let's start with: an example to make the following implementation more clear:

        We have a Snowpark Pandas DataFrame:
        df = pd.DataFrame([['k0', 13, 'd'], ['k1', 14, 'b'], ['k0', 15, 'c']], index=pd.MultiIndex.from_tuples([(1, 3),  (1, 2), (0, 0)], names=['i1', 'i2']), columns=pd.MultiIndex.from_tuples([('a', 'group_key'), ('b', 'int_col'), ('b', 'string_col')], names=['c1', 'c2']))

        looks like:

                c1            a       b
        c2    group_key int_col string_col
        i1 i2
        1  3         k0      13          d
        1  2         k1      14          b
        0  0         k0      15          c

        df.groupby(['i1', ('a', 'group_key')], group_keys=True).apply(lambda grp: native_pd.concat([grp, grp * 2]) if grp.iloc[0,0] == 'k1' else grp)


        result looks like:

        c1                              a       b
        c2                      group_key int_col string_col
        i1 (a, group_key) i1 i2
        0  k0             0  0         k0      15          c
        1  k0             1  3         k0      13          d
           k1             1  2         k1      14          b
                             2       k1k1      28         bb

        """

        ordered_dataframe = ordered_dataframe.ensure_row_position_column()
        row_position_snowflake_quoted_identifier = (
            ordered_dataframe.row_position_snowflake_quoted_identifier
        )
        """
        ordered_dataframe starts like this:

        |   __i1__ |   __i2__ | ('a', 'group_key')   |   ('b', 'int_col') | ('b', 'string_col')   |   __row_position__ |
        |---------:|---------:|:---------------------|-------------------:|:----------------------|-------------------:|
        |        1 |        3 | k0                   |                 13 | d                     |                  0 |
        |        1 |        2 | k1                   |                 14 | b                     |                  1 |
        |        0 |        0 | k0                   |                 15 | c                     |                  2 |
        """
        if output_schema is not None:
            x = udtf(
                row_position_snowflake_quoted_identifier,
                *by_snowflake_quoted_identifiers_list,
                *new_internal_df.index_column_snowflake_quoted_identifiers,
                *input_data_column_identifiers,
            ).over(
                partition_by=None
                if force_single_group
                else [*by_snowflake_quoted_identifiers_list],
                order_by=row_position_snowflake_quoted_identifier,
            )
            ordered_dataframe = ordered_dataframe.select(x)
            num_by = len(by_snowflake_quoted_identifiers_list)
            result_frame = create_internal_frame_for_groupby_apply_no_pivot_result(
                _modin_frame,
                ordered_dataframe,
                output_schema,
                num_by,
                is_transform,
                group_keys,
                as_index,
                sort,
            )
            return SnowflakeQueryCompiler(result_frame)

        # NOTE we are keeping the cache_result for performance reasons. DO NOT
        # REMOVE the cache_result unless you can prove that doing so will not
        # materially slow down CI or individual groupby.apply() calls.
        # TODO(SNOW-1345395): Investigate why and to what extent the cache_result
        # is useful.
        ordered_dataframe = cache_result(
            ordered_dataframe.select(
                *by_snowflake_quoted_identifiers_list,
                udtf(
                    row_position_snowflake_quoted_identifier,
                    *by_snowflake_quoted_identifiers_list,
                    *new_internal_df.index_column_snowflake_quoted_identifiers,
                    *input_data_column_identifiers,
                ).over(
                    partition_by=None
                    if force_single_group
                    else [*by_snowflake_quoted_identifiers_list],
                    order_by=row_position_snowflake_quoted_identifier,
                ),
            )
        )

        """
        After applying the udtf, the underlying Snowpark DataFrame contains the group keys, followed by columns representing the UDTF results:

        --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        |   __i1__  | ('a', 'group_key') | "original_row_position"   | "row_position_within_group"|"LABEL"                                                               |"VALUE"| "first_group_key_occurence_position"
        --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        |     0    |       k0            |         2                 |           0                |{"index_pos": 0,  "name": "i1"}                                       | 0     | 2
        |     0    |       k0            |         2                 |           0                |{"index_pos": 1,  "name": "i2"}                                       | 0     | 2
        |     0    |       k0            |         2                 |           0                |{"data_pos": 0,  "0": "a", "1": "group_key", "names": ["c1", "c2"]}   | k0    | 2
        |     0    |       k0            |         2                 |           0                |{"data_pos": 1,  "0": "b", "1": "int_col", "names": ["c1", "c2"]}     | 15    | 2
        |     0    |       k0            |         2                 |           0                |{"data_pos": 2,  "0": "b", "1": "string_col", "names": ["c1", "c2"]}  | c     | 2
        |     1    |       k0            |         0                 |           0                |{"index_pos": 0,  "name": "i1"}                                       | 1     | 0
        |     1    |       k0            |         0                 |           0                |{"index_pos": 1,  "name": "i2"}                                       | 3     | 0
        |     1    |       k0            |         0                 |           0                |{"data_pos": 0,  "0": "a", "1": "group_key", "names": ["c1", "c2"]}   | k0    | 0
        |     1    |       k0            |         0                 |           0                |{"data_pos": 1,  "0": "b", "1": "int_col", "names": ["c1", "c2"]}     | 13    | 0
        |     1    |       k0            |         0                 |           0                |{"data_pos": 2,  "0": "b", "1": "string_col", "names": ["c1", "c2"]}  | d     | 0
        |     1    |       k1            |         -1                |           0                |{"index_pos": 0,  "name": "i1"}                                       | 1     | 1
        |     1    |       k1            |         -1                |           0                |{"index_pos": 1,  "name": "i2"}                                       | 2     | 1
        |     1    |       k1            |         -1                |           0                |{"data_pos": 0,  "0": "a", "1": "group_key", "names": ["c1", "c2"]}   | k1    | 1
        |     1    |       k1            |         -1                |           0                |{"data_pos": 1,  "0": "b", "1": "int_col", "names": ["c1", "c2"]}     | 14    | 1
        |     1    |       k1            |         -1                |           0                |{"data_pos": 2,  "0": "b", "1": "string_col", "names": ["c1", "c2"]}  | b     | 1
        |     1    |       k1            |         -1                |           1                |{"index_pos": 0,  "name": "i1"}                                       | 1     | 1
        |     1    |       k1            |         -1                |           1                |"index_pos": 1,  "name": "i2"}                                        | 2     | 1
        |     1    |       k1            |         -1                |           1                |{"data_pos": 0,  "0": "a", "1": "group_key", "names": ["c1", "c2"]}   | k1k1  | 1
        |     1    |       k1            |         -1                |           1                |{"data_pos": 1,  "0": "b", "1": "int_col", "names": ["c1", "c2"]}     | 28    | 1
        |     1    |       k1            |         -1                |           1                |{"data_pos": 2,  "0": "b", "1": "string_col", "names": ["c1", "c2"]}  | bb    | 1
        --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

        Observe:
        - For each final output row, there are 5 entries in this table, because
          each output row has two index levels and 3 data columns.
        - The function acted as a transform on the groups with keys (0, 'k0') and
          (1, 'k0'), so "original_row_position" has non-negative indices for
          the results on those groups. However, the function did not act as a
          transform on the group with key (1, 'k1'), since the output has more
          rows than the input. "original_row_position" is -1 for all rows
          resulting from that group.
        - "first_group_key_occurence_position" is 2 for rows coming from group key
          (0, 'k0'), because that key first occurs in row 2 of the original dataframe.
          Likewise, (1, 'k0') gets "first_group_key_occurence_position" of 0 because
          it occurs in row 0 of the original frame, and (1, 'k1') gets
          "first_group_key_occurence_position" of 1 because it first occurs in row 1
          of the original frame.
        """

        ordered_dataframe = ordered_dataframe.pivot(
            APPLY_LABEL_COLUMN_QUOTED_IDENTIFIER,
            None,
            None,
            min_(APPLY_VALUE_COLUMN_QUOTED_IDENTIFIER),
        )

        """
        The pivot rotates the `func` results into separate columns, with one
        column for each index level and each data column. The result contains
        the by columns, then some metadata columns, then the pivoted `func`
        result columns and index levels.

        ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        |   __i1__ | ('a', 'group_key')   | "original_row_position" | "row_position_within_group"  | "first_group_key_occurence_position"    | {"index_pos": 0,  "name": "i1"} |  {"index_pos": 1,  "name": "i2"} |  {"data_pos": 0,  "0": "a", "1": "group_key", "names": ["c1", "c2"]}  | {"data_pos": "1",  "0": "b", "1": "int_col", "names": ["c1", "c2"]}  | {"data_pos": "2",  "0": "b", "1": "string_col", "names": ["c1", "c2"]}  |
        ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        |     0    |       k0            |           2              |0                             |2                                        | 0                               |    0                             | c                                                                     | 15                                                                   | c
        |     1    |       k0            |           0              |0                             |0                                        | 1                               |    3                             | d                                                                     | 13                                                                   | d
        |     1    |       k1            |           1              |0                             |1                                        | 1                               |    2                             | b                                                                     | 14                                                                   | b
        |     1    |       k1            |           1              |1                             |1                                        | 1                               |    2                             | bb                                                                    | 28                                                                   | b
        ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        """

        pivot_result_data_column_snowflake_quoted_identifiers = (
            ordered_dataframe.projected_column_snowflake_quoted_identifiers
        )
        num_by_columns = len(by_snowflake_quoted_identifiers_list)
        # The following 3 columns appear after the by columns, so get their
        # identifiers by looking at the 3 column names that follow the by
        # column names.
        (
            row_position_within_group_snowflake_quoted_identifier,
            original_row_position_snowflake_quoted_identifier,
            group_key_appearance_order_quoted_identifier,
        ) = pivot_result_data_column_snowflake_quoted_identifiers[
            num_by_columns : num_by_columns + 3
        ]

        (
            column_index_names,
            data_column_pandas_labels,
            data_column_snowflake_quoted_identifiers,
            index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers,
        ) = get_metadata_from_groupby_apply_pivot_result_column_names(
            pivot_result_data_column_snowflake_quoted_identifiers[
                # the rest of the pivot result's columns represent the index and
                # data columns of calling func() on each group.
                (num_by_columns + 3) :
            ]
        )
        # Only when func returns a dataframe does the pivot result include
        # index columns.
        func_returned_dataframe = len(index_column_pandas_labels) > 0

        # Generate quoted identifiers for the index and data columns.
        renamed_data_column_snowflake_quoted_identifiers = (
            ordered_dataframe.generate_snowflake_quoted_identifiers(
                pandas_labels=data_column_pandas_labels,
            )
        )
        renamed_index_column_snowflake_quoted_identifiers = (
            ordered_dataframe.generate_snowflake_quoted_identifiers(
                pandas_labels=index_column_pandas_labels,
                excluded=renamed_data_column_snowflake_quoted_identifiers,
            )
        )
        # this is the identifier for the new index column that we'll need to
        # add if as_index=False.
        new_index_identifier = ordered_dataframe.generate_snowflake_quoted_identifiers(
            pandas_labels=[None],
            excluded=[
                *renamed_data_column_snowflake_quoted_identifiers,
                *renamed_index_column_snowflake_quoted_identifiers,
            ],
        )[0]

        if func_returned_dataframe:
            # follow pandas behavior: when `func` returns a dataframe, respect
            # as_index=False if and only if group_keys=True.
            # not sure whether that's a pandas bug:
            # https://github.com/pandas-dev/pandas/issues/57656
            if not as_index and not group_keys:
                as_index = True
            else:
                as_index = as_index

        ordered_dataframe = groupby_apply_pivot_result_to_final_ordered_dataframe(
            ordered_dataframe=ordered_dataframe,
            agg_func=agg_func,
            by_snowflake_quoted_identifiers_list=by_snowflake_quoted_identifiers_list,
            sort_method=GroupbyApplySortMethod.ORIGINAL_ROW_ORDER
            if force_single_group
            else groupby_apply_sort_method(
                sort,
                group_keys,
                original_row_position_snowflake_quoted_identifier,
                ordered_dataframe,
                func_returned_dataframe,
            ),
            as_index=as_index,
            original_row_position_snowflake_quoted_identifier=original_row_position_snowflake_quoted_identifier,
            group_key_appearance_order_quoted_identifier=group_key_appearance_order_quoted_identifier,
            row_position_within_group_snowflake_quoted_identifier=row_position_within_group_snowflake_quoted_identifier,
            data_column_snowflake_quoted_identifiers=data_column_snowflake_quoted_identifiers,
            index_column_snowflake_quoted_identifiers=index_column_snowflake_quoted_identifiers,
            renamed_data_column_snowflake_quoted_identifiers=renamed_data_column_snowflake_quoted_identifiers,
            renamed_index_column_snowflake_quoted_identifiers=renamed_index_column_snowflake_quoted_identifiers,
            new_index_identifier=new_index_identifier,
            func_returned_dataframe=func_returned_dataframe,
        )
        return SnowflakeQueryCompiler(
            groupby_apply_create_internal_frame_from_final_ordered_dataframe(
                ordered_dataframe=ordered_dataframe,
                func_returned_dataframe=func_returned_dataframe,
                as_index=as_index,
                group_keys=group_keys,
                by_pandas_labels=by_pandas_labels,
                by_snowflake_quoted_identifiers=by_snowflake_quoted_identifiers_list,
                func_result_data_column_pandas_labels=data_column_pandas_labels,
                func_result_data_column_snowflake_quoted_identifiers=renamed_data_column_snowflake_quoted_identifiers,
                func_result_index_column_pandas_labels=index_column_pandas_labels,
                func_result_index_column_snowflake_quoted_identifiers=renamed_index_column_snowflake_quoted_identifiers,
                column_index_names=column_index_names,
                new_index_identifier=new_index_identifier,
                original_data_column_pandas_labels=self._modin_frame.data_column_pandas_labels,
            )
        )

    def _fill_null_values_in_groupby(
        self, method: str, by_list: list[str], limit: Optional[int] = None
    ) -> dict[str, ColumnOrName]:
        """
        Fill null values in each column using method within each group.

        Args:
            method: "bfill" or "ffill"
                The method to use to fill null values.
            by_list: list[str]
                The list of columns to partition by during the fillna.
            limit : int, optional
                The limit of values in a run to fill.

        Returns:
            dict: A mapping between column name and the Snowpark Column object with
                replaced null values.
        """
        method = FillNAMethod.get_enum_for_string_method(method)
        method_is_ffill = method is FillNAMethod.FFILL_METHOD
        if method_is_ffill:
            func = last_value
            window_start = Window.UNBOUNDED_PRECEDING if limit is None else -1 * limit
            window_end = Window.CURRENT_ROW
        else:
            func = first_value
            window_start = Window.CURRENT_ROW
            window_end = Window.UNBOUNDED_FOLLOWING if limit is None else limit

        return {
            snowflake_quoted_id: coalesce(
                snowflake_quoted_id,
                func(snowflake_quoted_id, ignore_nulls=True).over(
                    Window.partition_by(by_list)
                    .order_by(
                        self._modin_frame.ordering_column_snowflake_quoted_identifiers
                    )
                    .rows_between(window_start, window_end)
                ),
            )
            for snowflake_quoted_id in self._modin_frame.data_column_snowflake_quoted_identifiers
        }

    def _groupby_first_last(
        self,
        method: str,
        by: Any,
        axis: int,
        groupby_kwargs: dict[str, Any],
        agg_args: tuple[Any],
        agg_kwargs: dict[str, Any],
        drop: bool = False,
        **kwargs: dict[str, Any],
    ) -> "SnowflakeQueryCompiler":
        """
        Get the first or last non-null value for each group.

        Args:
            by: mapping, series, callable, label, pd.Grouper, BaseQueryCompiler, list of such.
                Use this to determine the groups.
            axis: 0 (index) or 1 (columns).
            groupby_kwargs: dict
                keyword arguments passed for the groupby.
            agg_args: tuple
                The aggregation args, unused in `groupby_size`.
            agg_kwargs: dict
                The aggregation keyword args, unused in `groupby_size`.
            drop: bool
                Drop the `by` column, unused in `groupby_size`.

        Returns:
            SnowflakeQueryCompiler: The result of groupby_first() or groupby_last()
        """
        level = groupby_kwargs.get("level", None)
        is_supported = check_is_groupby_supported_by_snowflake(by, level, axis)
        if not is_supported:
            ErrorMessage.not_implemented(
                f"Snowpark pandas GroupBy.{method} {_GROUPBY_UNSUPPORTED_GROUPING_MESSAGE}"
            )
        # TODO: Support groupby first and last with min_count (SNOW-1482931)
        if agg_kwargs.get("min_count", -1) > 1:
            ErrorMessage.not_implemented(
                f"Snowpark pandas GroupBy.{method} does not yet support min_count"
            )
        sort = groupby_kwargs.get("sort", True)
        as_index = groupby_kwargs.get("as_index", True)
        fillna_method = "bfill" if method == "first" else "ffill"
        query_compiler, by_list = resample_and_extract_groupby_column_pandas_labels(
            self, by, level
        )
        by_snowflake_quoted_identifiers_list = [
            entry[0]
            for entry in self._modin_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                by_list
            )
        ]
        if not agg_kwargs.get("skipna", True):
            # If we don't skip nulls, we don't need to fillna.
            result = query_compiler
        else:
            result = SnowflakeQueryCompiler(
                query_compiler._modin_frame.update_snowflake_quoted_identifiers_with_expressions(
                    quoted_identifier_to_column_map=self._fill_null_values_in_groupby(
                        fillna_method, by_snowflake_quoted_identifiers_list
                    ),
                    snowpark_pandas_types=self._modin_frame.cached_data_column_snowpark_pandas_types,
                ).frame
            )
        result = result.groupby_agg(
            by, "head" if method == "first" else "tail", 0, groupby_kwargs, (), {"n": 1}
        )
        if sort:
            result = result.sort_rows_by_column_values(
                by_list, [True] * len(by_list), "stable", "last", False
            )
        # set the index to position the columns correctly.
        result = result.set_index(by_list)
        if not as_index:
            # _groupby_head_tail keeps old positions, so we drop those and generate new ones
            result = result.reset_index(drop=False)
        return result

    def groupby_first(
        self,
        by: Any,
        axis: int,
        groupby_kwargs: dict[str, Any],
        agg_args: tuple[Any],
        agg_kwargs: dict[str, Any],
        drop: bool = False,
        **kwargs: dict[str, Any],
    ) -> "SnowflakeQueryCompiler":
        """
        Get the first non-null value for each group.

        Args:
            by: mapping, series, callable, label, pd.Grouper, BaseQueryCompiler, list of such.
                Use this to determine the groups.
            axis: 0 (index) or 1 (columns).
            groupby_kwargs: dict
                keyword arguments passed for the groupby.
            agg_args: tuple
                The aggregation args, unused in `groupby_size`.
            agg_kwargs: dict
                The aggregation keyword args, unused in `groupby_size`.
            drop: bool
                Drop the `by` column, unused in `groupby_size`.

        Returns:
            SnowflakeQueryCompiler: The result of groupby_first()
        """
        return self._groupby_first_last(
            "first", by, axis, groupby_kwargs, agg_args, agg_kwargs, drop, **kwargs
        )

    def groupby_last(
        self,
        by: Any,
        axis: int,
        groupby_kwargs: dict[str, Any],
        agg_args: tuple[Any],
        agg_kwargs: dict[str, Any],
        drop: bool = False,
        **kwargs: dict[str, Any],
    ) -> "SnowflakeQueryCompiler":
        """
        Get the last non-null value for each group.

        Args:
            by: mapping, series, callable, label, pd.Grouper, BaseQueryCompiler, list of such.
                Use this to determine the groups.
            axis: 0 (index) or 1 (columns).
            groupby_kwargs: dict
                keyword arguments passed for the groupby.
            agg_args: tuple
                The aggregation args, unused in `groupby_size`.
            agg_kwargs: dict
                The aggregation keyword args, unused in `groupby_size`.
            drop: bool
                Drop the `by` column, unused in `groupby_size`.

        Returns:
            SnowflakeQueryCompiler: The result of groupby_last()
        """
        return self._groupby_first_last(
            "last", by, axis, groupby_kwargs, agg_args, agg_kwargs, drop, **kwargs
        )

    def groupby_rank(
        self,
        by: Any,
        groupby_kwargs: dict[str, Any],
        agg_args: Any,
        agg_kwargs: dict[str, Any],
        axis: Axis = 0,
        method: Literal["average", "min", "max", "first", "dense"] = "average",
        na_option: Literal["keep", "top", "bottom"] = "keep",
        ascending: bool = True,
        pct: bool = False,
    ) -> "SnowflakeQueryCompiler":
        """
        Compute groupby with rank.

        Parameters
        ----------
        by:
            The columns or index levels to group by.
        axis: {0}
        method: {"average", "min", "max", "first", "dense"}
            How to rank the group of records that have the same value (i.e. break ties):
            - average: average rank of the group
            - min: lowest rank in the group
            - max: highest rank in the group
            - first: ranks assigned in order they appear in the array
            - dense: like 'min', but rank always increases by 1 between groups.
        na_option: {"keep", "top", "bottom"}
            How to rank NaN values:
            - keep: assign NaN rank to NaN values
            - top: assign lowest rank to NaN values
            - bottom: assign highest rank to NaN values
        ascending: bool
            Whether the elements should be ranked in ascending order.
        pct: bool
            Whether to display the returned rankings in percentile form.
        groupby_kwargs:
            Keyword arguments for the groupby object, i.e. for the df.groupby() call.
        agg_args:
            Positional arguments to pass to agg_func when applying it to each group.
        agg_kwargs:
            Keyword arguments to pass to agg_func when applying it to each group.

        Returns
        -------
            SnowflakeQueryCompiler: with a newly constructed internal dataframe

        Examples
        --------
        >>> df = pd.DataFrame({"group": ["a", "a", "a", "b", "b", "b", "b"], "value": [2, 4, 2, 3, 5, 1, 2]})
        >>> df
          group  value
        0     a	     2
        1     a	     4
        2     a	     2
        3     b      3
        4     b      5
        5     b      1
        6     b      2
        >>> df = df.groupby("group").rank(method='min')
        >>> df
           value
        0      1
        1      3
        2      1
        3      3
        4      4
        5      1
        6      2
        """
        self._raise_not_implemented_error_for_timedelta()

        level = groupby_kwargs.get("level", None)
        dropna = groupby_kwargs.get("dropna", True)

        if not check_is_groupby_supported_by_snowflake(by, level, axis):
            ErrorMessage.not_implemented(
                f"GroupBy rank with by = {by}, level = {level} and axis = {axis} is not supported yet in Snowpark pandas."
            )

        if level is not None and level != 0:
            ErrorMessage.not_implemented(
                f"GroupBy rank with level = {level} is not supported yet in Snowpark pandas."
            )

        query_compiler = self
        original_frame = query_compiler._modin_frame
        ordered_dataframe = original_frame.ordered_dataframe
        ordering_column_identifiers = (
            original_frame.ordering_column_snowflake_quoted_identifiers
        )

        query_compiler, by_list = resample_and_extract_groupby_column_pandas_labels(
            self, by, level
        )
        by_snowflake_quoted_identifiers_list = [
            entry[0]
            for entry in original_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                by_list
            )
        ]

        pandas_labels = []
        new_cols = []
        partition_list = by_snowflake_quoted_identifiers_list.copy()
        for col_label, col_ident in zip(
            original_frame.data_column_pandas_labels,
            original_frame.data_column_snowflake_quoted_identifiers,
        ):
            if col_ident not in by_snowflake_quoted_identifiers_list:
                count_alias = ordered_dataframe.generate_snowflake_quoted_identifiers(
                    pandas_labels=["c_" + col_label]
                )[0]
                # Partition by group columns and current data column
                partition_list.append(col_ident)

                # Frame to record count of non-null values
                count_df = ordered_dataframe.select(
                    col_ident,
                    count("*")
                    .over(Window.partition_by(partition_list))
                    .alias(count_alias),
                ).ensure_row_position_column()
                # Count value is used for calculating max and average rank from
                # min rank in function make_groupby_rank_col_for_method
                count_val = col(
                    count_df.projected_column_snowflake_quoted_identifiers[1]
                )

                # Resulting rank column
                rank_col = make_groupby_rank_col_for_method(
                    col_ident,
                    by_snowflake_quoted_identifiers_list,
                    method,
                    na_option,
                    ascending,
                    pct,
                    ordering_column_identifiers,
                    count_val,
                    dropna,
                )
                new_cols.append(rank_col)
                pandas_labels.append(col_label)
                partition_list.remove(col_ident)

        return SnowflakeQueryCompiler(
            query_compiler._modin_frame.project_columns(pandas_labels, new_cols)
        )

    def groupby_resample(
        self,
        resample_kwargs: dict[str, Any],
        resample_method: AggFuncType,
        groupby_kwargs: dict[str, Any],
        is_series: bool,
        agg_args: Any,
        agg_kwargs: dict[str, Any],
    ) -> "SnowflakeQueryCompiler":

        validate_groupby_resample_supported_by_snowflake(resample_kwargs)
        level = groupby_kwargs.get("level", None)
        by = groupby_kwargs.get("by", None)

        axis = resample_kwargs.get("axis", 0)
        rule = resample_kwargs.get("rule")
        on = resample_kwargs.get("on")

        if not check_is_groupby_supported_by_snowflake(by, level, axis):
            ErrorMessage.not_implemented(
                f"GroupBy resample with by = {by}, level = {level} and axis = {axis} is not supported yet in Snowpark pandas."
            )

        by_list = extract_groupby_column_pandas_labels(self, by, level)

        if on is not None:
            if on not in self._modin_frame.data_column_pandas_labels:
                raise KeyError(f"{on}")
            frame = self.set_index(keys=[on])._modin_frame
        else:
            frame = self._modin_frame
        snowflake_index_column_identifier = (
            get_snowflake_quoted_identifier_for_resample_index_col(frame)
        )
        orig_datetime_index_col_label = frame.index_column_pandas_labels[0]
        slice_width, slice_unit = rule_to_snowflake_width_and_slice_unit(rule)

        start_date, end_date = compute_resample_start_and_end_date(
            frame,
            snowflake_index_column_identifier,
            rule,
        )
        # Say this is the original frame with start_date = 2000-01-01 00:00:00 and end_date = 2000-01-01 00:07:00
        #                      a  b  c
        # index
        # 2000-01-01 00:00:00  0  1  2
        # 2000-01-01 00:01:00  0  1  2
        # 2000-01-01 00:02:00  5  1  2
        # 2000-01-01 00:06:00  0  1  2
        # 2000-01-01 00:07:00  5  1  2

        if resample_method in IMPLEMENTED_AGG_METHODS:
            resampled_frame = perform_resample_binning_on_frame(
                frame=frame,
                datetime_index_col_identifier=snowflake_index_column_identifier,
                start_date=start_date,
                slice_width=slice_width,
                slice_unit=slice_unit,
            )

            # resampled_frame is the frame with index column items set to its resampled bin
            # With rule='3min', resampled_frame will look like this:

            #                      a  b  c
            # index
            # 2000-01-01 00:00:00  0  1  2
            # 2000-01-01 00:00:00  0  1  2
            # 2000-01-01 00:00:00  5  1  2
            # 2000-01-01 00:06:00  0  1  2
            # 2000-01-01 00:06:00  5  1  2

            agg_by_list = by_list + frame.index_column_pandas_labels
            qc = SnowflakeQueryCompiler(resampled_frame).groupby_agg(
                by=agg_by_list,
                agg_func=resample_method,
                axis=axis,
                groupby_kwargs=dict(),
                agg_args=agg_args,
                agg_kwargs=agg_kwargs,
                numeric_only=agg_kwargs.get("numeric_only", False),
                is_series_groupby=is_series,
            )

        # after the groupby_agg grouping by 'a', the frame will look like this:
        #                        b  c
        # a index
        # 0 2000-01-01 00:00:00  2  4
        #   2000-01-01 00:06:00  1  2
        # 5 2000-01-01 00:00:00  1  2
        #   2000-01-01 00:06:00  1  2

        quoted_by_list = (
            qc._modin_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                by_list
            )
        )
        resampled_quoted_ids = qc._modin_frame.index_column_snowflake_quoted_identifiers

        for x in quoted_by_list:
            if x[0] in resampled_quoted_ids:
                resampled_quoted_ids.remove(x[0])

        frame = qc._modin_frame
        datetime_index_col_identifier = resampled_quoted_ids[0]

        # the aggregated frame with index column items set to its resampled bin may still be missing resampled bins
        # based on the frequency.
        # fill_missing_groupby_resample_bins_for_frame will fill in those missing bins with NaN values.

        resampled_frame_all_bins = fill_missing_groupby_resample_bins_for_frame(
            frame,
            rule,
            by_list,
            orig_datetime_index_col_label,
            datetime_index_col_identifier,
        )

        # after filling in the missing bins, the frame will look like this:

        #                        b  c
        # a index
        # 0 2000-01-01 00:00:00  2  4
        #   2000-01-01 00:03:00  NaN  NaN
        #   2000-01-01 00:06:00  1  2
        # 5 2000-01-01 00:00:00  1  2
        #   2000-01-01 00:03:00  NaN  NaN
        #   2000-01-01 00:06:00  1  2

        if resample_method in ("sum", "count", "size", "nunique"):
            values_arg: Union[int, dict]
            if resample_method == "sum":
                values_arg = {}
                for pandas_label in resampled_frame_all_bins.data_column_pandas_labels:
                    label_dtypes: native_pd.Series = self.dtypes[[pandas_label]]
                    values_arg[pandas_label] = (
                        native_pd.Timedelta(0)
                        if len(set(label_dtypes)) == 1
                        and is_timedelta64_dtype(label_dtypes.iloc[0])
                        else 0
                    )
                if is_series:
                    # For series, fillna() can't handle a dictionary, but
                    # there should only be one column, so pass a scalar fill
                    # value.
                    assert len(values_arg) == 1
                    values_arg = list(values_arg.values())[0]
            else:
                values_arg = 0
            return SnowflakeQueryCompiler(resampled_frame_all_bins).fillna(
                value=values_arg, self_is_series=is_series
            )
        return SnowflakeQueryCompiler(resampled_frame_all_bins)

    def groupby_shift(
        self,
        by: Any,
        axis: int,
        level: int,
        periods: int,
        freq: str,
        fill_value: Any,
        is_series_groupby: bool,
    ) -> "SnowflakeQueryCompiler":
        """
        compute groupby with shift.
        Note: this variant of pandas groupby is more of a window based LEAD/LAG calculation than a GROUPBY in SQL
        With a dataframe created with following:
        import pandas as pd

        data = [[1,2,3], [1, 5, 6], [2, 5, 8], [2, 6, 9]]

        df = pd.DataFrame(data, columns=["a", "b", "c"], index = ["tuna", "salmon", "catfish", "goldfish"])

        df

                  a  b  c

        tuna      1  2  3
        salmon    1  5  6
        catfish   2  5  8
        goldfish  2  6  9

        df.groupby("a").shift(1)

                    b    c

        tuna      NaN  NaN
        salmon    2.0  3.0
        catfish   NaN  NaN
        goldfish  5.0  8.0

        Note that the type of the data has changed to decimal - this might be because of the need
        to introduce NULLs.

        data = [1, 2, 3, 4, 5]

        df = pd.DataFrame(data, columns=["a", "b", "c"], index = ["tuna", "salmon", "catfish", "goldfish"])

        df

                  a  b  c

        tuna      1  2  3
        salmon    1  5  6
        catfish   2  5  8
        goldfish  2  6  9

        df.groupby("a").shift(1)

                    b    c

        tuna      NaN  NaN
        salmon    2.0  3.0
        catfish   NaN  NaN
        goldfish  5.0  8.0

        In [2]: data = [1,2,3,4,5]

        In [3]: index = ["tuna", "salmon", "catfish", "goldfish", "promfret"]

        In [4]: series = pd.Series(data=data, index=index)

        In [5]: series
        Out[5]:
        tuna        1
        salmon      2
        catfish     3
        goldfish    4
        promfret    5
        dtype: int64

        In [6]: series.groupby(level=0).shift(3)
        Out[6]:
        tuna       NaN
        salmon     NaN
        catfish    NaN
        goldfish   NaN
        promfret   NaN
        dtype: float64


        Args:
            periods: Number of periods to shift by.
            freq: the frequency specified as a string.
            axis: 0 (index), 1 (columns)
            fill_value: Value to use in place of missing values.
            suffix: disambiguating columns if multiple periods are specified.
        Returns:
            SnowflakeQueryCompiler: with a newly constructed internal dataframe
        """
        self._raise_not_implemented_error_for_timedelta()

        # TODO: handle cases where the fill_value has a different type from
        # the column. SNOW-990325 deals with fillna that has a similar problem.

        if not isinstance(periods, int):
            if isinstance(periods, float):
                if not periods.is_integer():
                    raise TypeError("an integer is required for periods")
            else:
                raise TypeError("an integer is required for periods")

        # TODO: SNOW-1006626 tracks follow on work for supporting Multiindex
        if self._modin_frame.is_multiindex():
            ErrorMessage.not_implemented(
                "GroupBy Shift with multi index is not supported yet in Snowpark pandas."
            )

        # TODO: SNOW-1006626 tracks follow on work for supporting External by
        if isinstance(by, list):
            if any(
                by_element
                for by_element in by
                if by_element not in self._modin_frame.data_column_pandas_labels
            ):
                ErrorMessage.not_implemented(
                    "GroupBy Shift with external by is not supported yet in Snowpark pandas."
                )

        if not check_is_groupby_supported_by_snowflake(by, level, axis):
            ErrorMessage.not_implemented(
                f"GroupBy Shift with by = {by}, level = {level} and axis = {axis} is not supported yet in Snowpark pandas."
            )

        # TODO: SNOW-1006626 tracks follow on work for supporting these parameters
        if (level is not None and level != 0) or axis != 0 or freq is not None:
            ErrorMessage.not_implemented(
                "GroupBy Shift with parameter axis != 0, freq != None, "
                + "level != None, sort, dropna or observed is not supported yet in Snowpark pandas."
            )

        query_compiler, by_list = resample_and_extract_groupby_column_pandas_labels(
            self, by, level
        )
        _modin_frame = query_compiler._modin_frame

        # TODO: SNOW-1006626 should fix this.
        if (
            not is_series_groupby
            and _modin_frame.index_column_pandas_labels is not None
            and by_list is not None
            and len(by_list) > 0
            and any(
                by_column in _modin_frame.index_column_pandas_labels
                for by_column in by_list
            )
        ):
            ErrorMessage.not_implemented(
                "GroupBy Shift with a by parameter column that is part of the index is not supported yet in Snowpark pandas."
            )

        func = lead if periods < 0 else lag
        periods = abs(periods)

        by_snowflake_quoted_identifiers_list = [
            entry[0]
            for entry in _modin_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                by_list
            )
        ]

        pandas_labels = []
        if periods != 0:
            new_columns = []
            for pandas_label, snowflake_quoted_identifier in zip(
                _modin_frame.data_column_pandas_labels,
                _modin_frame.data_column_snowflake_quoted_identifiers,
            ):
                if (
                    snowflake_quoted_identifier
                    not in by_snowflake_quoted_identifiers_list
                ):
                    window = Window.partition_by(
                        by_snowflake_quoted_identifiers_list
                    ).order_by(
                        _modin_frame.ordered_dataframe.ordering_column_snowflake_quoted_identifiers
                    )

                    new_col = func(
                        snowflake_quoted_identifier, periods, fill_value
                    ).over(window)

                    pandas_labels.append(pandas_label)
                    new_columns.append(new_col)
            return SnowflakeQueryCompiler(
                _modin_frame.project_columns(pandas_labels, new_columns)
            )

        snowflake_quoted_identifiers = []
        for pandas_label, col_name in zip(
            _modin_frame.data_column_pandas_labels,
            _modin_frame.data_column_snowflake_quoted_identifiers,
        ):
            if col_name not in by_snowflake_quoted_identifiers_list:
                snowflake_quoted_identifiers.append(col_name)
                pandas_labels.append(pandas_label)

        new_ordered_dataframe = _modin_frame.ordered_dataframe.select(
            snowflake_quoted_identifiers
            + _modin_frame.index_column_snowflake_quoted_identifiers
        )
        return SnowflakeQueryCompiler(
            InternalFrame.create(
                ordered_dataframe=new_ordered_dataframe,
                data_column_pandas_labels=pandas_labels,
                data_column_pandas_index_names=self._modin_frame.data_column_pandas_index_names,
                data_column_snowflake_quoted_identifiers=snowflake_quoted_identifiers,
                index_column_pandas_labels=self._modin_frame.index_column_pandas_labels,
                index_column_snowflake_quoted_identifiers=self._modin_frame.index_column_snowflake_quoted_identifiers,
                data_column_types=None,
                index_column_types=None,
            )
        )

    def groupby_get_group(
        self,
        by: Any,
        axis: int,
        groupby_kwargs: dict[str, Any],
        agg_args: tuple[Any],
        agg_kwargs: dict[str, Any],
        drop: bool = False,
        **kwargs: dict[str, Any],
    ) -> "SnowflakeQueryCompiler":
        """
        Get all rows that match a given group name in the `by` column.

        Arguments:
            by: mapping, series, callable, label, pd.Grouper, BaseQueryCompiler, list of such.
                Use this to determine the groups.
            axis: 0 (index) or 1 (columns)
            groupby_kwargs: dict
                keyword arguments passed for the groupby.
            agg_args: tuple
                The aggregation args, unused in `groupby_get_group`.
            agg_kwargs: dict
                The aggregation keyword args, holds the name parameter.
            drop: bool
                Drop the `by` column, unused in `groupby_get_group`.
        Returns:
            SnowflakeQueryCompiler: The result of groupby_get_group().
        """
        self._raise_not_implemented_error_for_timedelta()

        level = groupby_kwargs.get("level", None)
        is_supported = check_is_groupby_supported_by_snowflake(by, level, axis)
        if not is_supported:  # pragma: no cover
            ErrorMessage.not_implemented(
                f"Snowpark pandas GroupBy.get_group {_GROUPBY_UNSUPPORTED_GROUPING_MESSAGE}"
            )
        if is_list_like(by):
            ErrorMessage.not_implemented(
                "Snowpark pandas GroupBy.get_group does not yet support multiple by columns."
            )
        name = agg_kwargs.get("name")
        return self.take_2d_labels(
            self.take_2d_labels(slice(None), by).binary_op("eq", name, 0),
            slice(None),
        )

    def groupby_size(
        self,
        by: Any,
        axis: int,
        groupby_kwargs: dict[str, Any],
        agg_args: tuple[Any],
        agg_kwargs: dict[str, Any],
        drop: bool = False,
        **kwargs: dict[str, Any],
    ) -> "SnowflakeQueryCompiler":
        """
        compute groupby with size.
        With a dataframe created with following:
        import pandas as pd

        data = [[1,2,3], [1, 5, 6], [2, 5, 8], [2, 6, 9]]

        df = pd.DataFrame(data, columns=["a", "b", "c"], index = ["tuna", "salmon", "catfish", "goldfish"])

        df

                  a  b  c

        tuna      1  2  3
        salmon    1  5  6
        catfish   2  5  8
        goldfish  2  6  9

        df.groupby("a").size()

        a
        1    2
        2    2
        dtype: int64


        Args:
            by: mapping, series, callable, label, pd.Grouper, BaseQueryCompiler, list of such.
                Use this to determine the groups.
            axis: 0 (index) or 1 (columns).
            groupby_kwargs: dict
                keyword arguments passed for the groupby.
            agg_args: tuple
                The aggregation args, unused in `groupby_size`.
            agg_kwargs: dict
                The aggregation keyword args, unused in `groupby_size`.
            drop: bool
                Drop the `by` column, unused in `groupby_size`.
        Returns:
            SnowflakeQueryCompiler: The result of groupby_size()
        """
        level = groupby_kwargs.get("level", None)
        is_supported = check_is_groupby_supported_by_snowflake(by, level, axis)
        if not is_supported:
            ErrorMessage.not_implemented(
                f"Snowpark pandas GroupBy.size {_GROUPBY_UNSUPPORTED_GROUPING_MESSAGE}"
            )
        if not is_list_like(by):
            by = [by]
        query_compiler, by = resample_and_extract_groupby_column_pandas_labels(
            self, by, level=level
        )
        positions_col_name = f"__TEMP_POS_NAME_{uuid.uuid4().hex[-6:]}__"
        # We reset index twice to ensure we perform the count aggregation on the row
        # positions (which cannot be null). We name the column a unique new name to
        # avoid collisions. We rename them to their final names at the end.
        result = (
            query_compiler.reset_index(drop=True)
            .reset_index(drop=False, names=positions_col_name)
            .take_2d_labels(slice(None), [positions_col_name] + by)
            .groupby_agg(
                by,
                "count",
                axis,
                groupby_kwargs,
                (),
                {},
            )
        )
        if not groupby_kwargs.get("as_index", True):
            return result.rename(columns_renamer={positions_col_name: "size"})
        else:
            return result.rename(
                columns_renamer={positions_col_name: MODIN_UNNAMED_SERIES_LABEL}
            )

    def groupby_groups(
        self,
        by: Any,
        axis: int,
        groupby_kwargs: dict[str, Any],
    ) -> PrettyDict[Hashable, "pd.Index"]:
        """
        Get a PrettyDict mapping group keys to row labels.

        Arguments:
            by: mapping, series, callable, label, pd.Grouper, BaseQueryCompiler, list of such.
                Use this to determine the groups.
            axis: 0 (index) or 1 (columns)
            groupby_kwargs: keyword arguments passed for the groupby.

        Returns:
            PrettyDict: a map from group keys to row labels.
        """

        """
        To get .groups, we have to group by the `by` columns / index levels
        as usual, and then aggregate the index columns into a list. Because
        groupby_agg() will only aggregate the data columns, and not the index
        columns, we copy the index columns into new data columns. We then
        aggregate those new data columns into arrays.

        In the comments below, we start with this example:

        >>> df = pd.DataFrame([[0, 1, 2], [4, 5, 2], [0, 8, 9]], columns=['col0', 'col1', 'col2']).set_index(['col0', 'col1'])
        >>> df
                    col2
        col0 col1
        0    1        2
        4    5        2
        0    8        9
        >>> df.groupby(by='col2').groups
        """

        """
        0. Copy the index columns into new data columns. After this step:

        >>> query_compiler.to_pandas()
                   col2  _snowpark_group_key0  _snowpark_group_key1
        col0 col1
        0    1        2                     0                     1
        4    5        2                     4                     5
        0    8        9                     0                     8
        """
        self._raise_not_implemented_error_for_timedelta()

        original_index_names = self.get_index_names()
        frame = self._modin_frame
        index_data_columns = []
        for i, index_identifier in enumerate(
            frame.index_column_snowflake_quoted_identifiers
        ):
            index_data_column = f"_snowpark_group_key{i}"
            index_data_columns.append(index_data_column)
            frame = frame.append_column(index_data_column, col(index_identifier))
        query_compiler = SnowflakeQueryCompiler(frame)

        """
        1. Now aggregate each index column separately into an array,
        and convert to pandas.

        After this step:

        >>> aggregated_as_pandas

                  _snowpark_group_key0   _snowpark_group_key1
            col2
            2                   [0, 4]                 [1, 5]
            9                      [0]                    [8]
        """
        aggregated_as_pandas = query_compiler.groupby_agg(
            by,
            {k: "array_agg" for k in index_data_columns},
            axis,
            groupby_kwargs,
            agg_args=[],
            agg_kwargs={},
        ).to_pandas()

        """
        2. Massage the resulting pandas dataframe into the final dictionary
        """
        return PrettyDict(
            # if the index has only one level, the dataframe has only one
            # column corresponding to the single level of the index. Convert
            # the dataframe to a series. e.g. turn
            #      _snowpark_group_key0
            #  2   [0, 4]
            #  9   [0]
            #
            # into {2: pd.Index([0, 4]), 9: pd.Index([0])}
            aggregated_as_pandas.iloc[:, 0].map(
                lambda v: native_pd.Index(
                    v,
                    # note that the index dtype has to match the original
                    # index's dtype, even if we could use a more restrictive
                    # type for this portion of the index.
                    dtype=self.index_dtypes[0],
                    name=original_index_names[0],
                )
            )
            if len(original_index_names) == 1
            # If there are multiple levels, each row represents that index
            # level's values for a particular group key. e.g.
            #      _snowpark_group_key0    _snowpark_group_key1
            #  2   [0, 4]                  [1, 5]
            #  9   [0]                     [8]
            #
            # for each row, we need to get a multiindex where level i of each
            # multiindex is equal to the _snowpark_group_key{i}. so for the
            # above example:
            # {2: pd.Index([(0, 1), (4, 5)]), 9: pd.Index([(0, 8)])
            else aggregated_as_pandas.apply(
                lambda row: pd.MultiIndex.from_arrays(
                    [
                        # note that the index dtype has to match the original
                        # index's dtype, even if we could use a more restrictive
                        # type for this portion of the index.
                        native_pd.Index(
                            row.iloc[i],
                            name=original_index_name,
                            dtype=index_dtype,
                        )
                        for i, (original_index_name, index_dtype) in enumerate(
                            zip(
                                original_index_names,
                                self.index_dtypes,
                            )
                        )
                    ]
                ),
                axis=1,
            )
        )

    def groupby_indices(
        self,
        by: Any,
        axis: int,
        groupby_kwargs: dict[str, Any],
        values_as_np_array: bool = True,
    ) -> dict[Hashable, np.ndarray]:
        """
        Get a dict mapping group keys to row labels.

        Arguments:
            by: mapping, series, callable, label, pd.Grouper, BaseQueryCompiler, list of such.
                Use this to determine the groups.
            axis: 0 (index) or 1 (columns)
            groupby_kwargs: keyword arguments passed for the groupby.
            values_as_np_array: bool, default True
                Whether the values of the resulting dict should be mapped as a numpy array.
                Set to False when called with 'resample.indices'.

        Returns:
            dict: a map from group keys to row labels.
        """
        self._raise_not_implemented_error_for_timedelta()

        frame = self._modin_frame.ensure_row_position_column()
        qc = (
            # .indices aggregates row position numbers, so we add a row
            # position data column and then aggregate that.
            SnowflakeQueryCompiler(
                frame.append_column(
                    "_snowpark_groupby_indices_position",
                    SnowparkColumn(frame.row_position_snowflake_quoted_identifier),
                )
            )
            .groupby_agg(
                by=by,
                agg_func={"_snowpark_groupby_indices_position": "array_agg"},
                axis=axis,
                groupby_kwargs=groupby_kwargs,
                agg_args=[],
                agg_kwargs={},
            )
            .to_pandas()
            .iloc[:, 0]
        )
        qc = qc.map(np.array) if values_as_np_array else qc
        return dict(qc)

    def groupby_cumcount(
        self,
        by: Any,
        axis: int,
        groupby_kwargs: dict[str, Any],
        ascending: bool,
    ) -> "SnowflakeQueryCompiler":
        """
        Number each item in each group from 0 to the length of that group - 1.

        Args:
            by: mapping, series, callable, label, pd.Grouper, BaseQueryCompiler, list of such.
                Used to determine the groups for the groupby.
            axis : 0 (index), 1 (columns)
            groupby_kwargs: Dict[str, Any]
                keyword arguments passed for the groupby.
            ascending : bool
                If False, number in reverse, from length of group - 1 to 0.

        Returns:
            SnowflakeQueryCompiler: with a newly constructed internal dataframe
        """
        self._raise_not_implemented_error_for_timedelta()

        return SnowflakeQueryCompiler(
            get_groupby_cumagg_frame_axis0(
                self,
                by=by,
                axis=axis,
                numeric_only=False,
                groupby_kwargs=groupby_kwargs,
                cumagg_func=count,
                cumagg_func_name="cumcount",
                ascending=ascending,
            )
        )

    def groupby_cummax(
        self,
        by: Any,
        axis: int,
        numeric_only: bool,
        groupby_kwargs: dict[str, Any],
    ) -> "SnowflakeQueryCompiler":
        """
        Cumulative max for each group.

        Args:
            by: mapping, series, callable, label, pd.Grouper, BaseQueryCompiler, list of such.
                Used to determine the groups for the groupby.
            axis : 0 (index), 1 (columns)
            numeric_only: bool
                Include only float, int, boolean columns.
            groupby_kwargs: Dict[str, Any]
                keyword arguments passed for the groupby.

        Returns:
            SnowflakeQueryCompiler: with a newly constructed internal dataframe
        """
        self._raise_not_implemented_error_for_timedelta()

        return SnowflakeQueryCompiler(
            get_groupby_cumagg_frame_axis0(
                self,
                by=by,
                axis=axis,
                numeric_only=numeric_only,
                groupby_kwargs=groupby_kwargs,
                cumagg_func=max_,
                cumagg_func_name="cummax",
            )
        )

    def groupby_cummin(
        self,
        by: Any,
        axis: int,
        numeric_only: int,
        groupby_kwargs: dict[str, Any],
    ) -> "SnowflakeQueryCompiler":
        """
        Cumulative min for each group.

        Args:
            by: mapping, series, callable, label, pd.Grouper, BaseQueryCompiler, list of such.
                Used to determine the groups for the groupby.
            axis : 0 (index), 1 (columns)
            numeric_only: bool
                Include only float, int, boolean columns.
            groupby_kwargs: Dict[str, Any]
                keyword arguments passed for the groupby.

        Returns:
            SnowflakeQueryCompiler: with a newly constructed internal dataframe
        """
        self._raise_not_implemented_error_for_timedelta()

        return SnowflakeQueryCompiler(
            get_groupby_cumagg_frame_axis0(
                self,
                by=by,
                axis=axis,
                numeric_only=numeric_only,
                groupby_kwargs=groupby_kwargs,
                cumagg_func=min_,
                cumagg_func_name="cummin",
            )
        )

    def groupby_cumsum(
        self,
        by: Any,
        axis: int,
        groupby_kwargs: dict[str, Any],
    ) -> "SnowflakeQueryCompiler":
        """
        Cumulative sum for each group.

        Args:
            by: mapping, series, callable, label, pd.Grouper, BaseQueryCompiler, list of such.
                Used to determine the groups for the groupby.
            axis : 0 (index), 1 (columns)
            groupby_kwargs: Dict[str, Any]
                keyword arguments passed for the groupby.

        Returns:
            SnowflakeQueryCompiler: with a newly constructed internal dataframe
        """
        self._raise_not_implemented_error_for_timedelta()

        return SnowflakeQueryCompiler(
            get_groupby_cumagg_frame_axis0(
                self,
                by=by,
                axis=axis,
                numeric_only=False,
                groupby_kwargs=groupby_kwargs,
                cumagg_func=sum_,
                cumagg_func_name="cumsum",
            )
        )

    def groupby_nunique(
        self,
        by: Any,
        axis: int,
        groupby_kwargs: dict[str, Any],
        agg_args: Any,
        agg_kwargs: dict[str, Any],
        drop: bool = False,
        **kwargs: Any,
    ) -> "SnowflakeQueryCompiler":
        # We have to override the Modin version of this function because our groupby frontend passes the
        # ignored numeric_only argument to this query compiler method, and BaseQueryCompiler
        # does not have **kwargs.
        return self.groupby_agg(
            by=by,
            agg_func="nunique",
            axis=axis,
            groupby_kwargs=groupby_kwargs,
            agg_args=agg_args,
            agg_kwargs=agg_kwargs,
            drop=drop,
        )

    def groupby_any(
        self,
        by: Any,
        axis: int,
        groupby_kwargs: dict[str, Any],
        agg_args: Any,
        agg_kwargs: dict[str, Any],
        drop: bool = False,
        **kwargs: Any,
    ) -> "SnowflakeQueryCompiler":

        # We have to override the Modin version of this function because our groupby frontend passes the
        # ignored numeric_only argument to this query compiler method, and BaseQueryCompiler
        # does not have **kwargs.
        return self.groupby_agg(
            by=by,
            agg_func="any",
            axis=axis,
            groupby_kwargs=groupby_kwargs,
            agg_args=agg_args,
            agg_kwargs=agg_kwargs,
            drop=drop,
        )

    def groupby_all(
        self,
        by: Any,
        axis: int,
        groupby_kwargs: dict[str, Any],
        agg_args: Any,
        agg_kwargs: dict[str, Any],
        drop: bool = False,
        **kwargs: Any,
    ) -> "SnowflakeQueryCompiler":

        # We have to override the Modin version of this function because our groupby frontend passes the
        # ignored numeric_only argument to this query compiler method, and BaseQueryCompiler
        # does not have **kwargs.
        return self.groupby_agg(
            by=by,
            agg_func="all",
            axis=axis,
            groupby_kwargs=groupby_kwargs,
            agg_args=agg_args,
            agg_kwargs=agg_kwargs,
            drop=drop,
        )

    def groupby_value_counts(
        self,
        by: Any,
        axis: int,
        groupby_kwargs: dict[str, Any],
        subset: Optional[list[str]],
        normalize: bool = False,
        sort: bool = True,
        ascending: bool = False,
        bins: Optional[int] = None,
        dropna: bool = True,
    ) -> "SnowflakeQueryCompiler":
        level = groupby_kwargs.get("level", None)
        as_index = groupby_kwargs.get("as_index", True)
        groupby_sort = groupby_kwargs.get("sort", True)
        is_supported = check_is_groupby_supported_by_snowflake(by, level, axis)
        if not is_supported:
            ErrorMessage.not_implemented(
                f"Snowpark pandas GroupBy.value_counts {_GROUPBY_UNSUPPORTED_GROUPING_MESSAGE}"
            )
        if bins is not None:
            raise ErrorMessage.not_implemented("bins argument is not yet supported")
        if not is_list_like(by):
            by = [by]
        if len(set(by) & set(subset or [])):
            # Check for overlap between by and subset. Since column names may contain customer data,
            # unlike pandas, we do not include the offending labels in the error message.
            raise ValueError("Keys in subset cannot be in the groupby column keys")
        if subset is not None:
            subset_list = subset
        else:
            # If subset is unspecified, then all columns should be included.
            subset_list = self._modin_frame.data_column_pandas_labels
        # The grouping columns are always included in the subset.
        # Furthermore, the columns of the output must have the grouping columns first, in the order
        # that they were specified.
        subset_list = by + list(filter(lambda label: label not in by, subset_list))

        if as_index:
            # When as_index=True, the result is a Series with a MultiIndex index.
            result = self._value_counts_groupby(
                by=subset_list,
                # Use sort=False to preserve the original order
                sort=False,
                normalize=normalize,
                ascending=False,
                dropna=dropna,
                normalize_within_groups=by,
            )
        else:
            # When as_index=False, the result is a DataFrame where count/proportion is appended as a new named column.
            result = self._value_counts_groupby(
                by=subset_list,
                # Use sort=False to preserve the original order
                sort=False,
                normalize=normalize,
                ascending=False,
                dropna=dropna,
                normalize_within_groups=by,
            ).reset_index()
            result = result.set_columns(
                result._modin_frame.data_column_pandas_labels[:-1]
                + ["proportion" if normalize else "count"]
            )
        # pandas currently provides the following behaviors based on the different sort flags.
        # These behaviors are not entirely consistent with documentation; see this issue for discussion:
        # https://github.com/pandas-dev/pandas/issues/59307
        #
        # Example data (using pandas 2.2.1 behavior):
        # >>> df = pd.DataFrame({"X": ["B", "A", "A", "B", "B", "B"], "Y": [4, 1, 3, -2, -1, -1]})
        #
        # 1. groupby(sort=True).value_counts(sort=True)
        #   Sort on non-grouping columns, then sort on frequencies, then sort on grouping columns.
        # >>> df.groupby("X", sort=True).value_counts(sort=True)
        # X  Y
        # A   1    1
        #     3    1
        # B  -1    2
        #    -2    1
        #     4    1
        # Name: count, dtype: int64
        #
        # 2. groupby(sort=True).value_counts(sort=False)
        #   Sort on non-grouping columns, then sort on grouping columns.
        # >>> df.groupby("X", sort=True).value_counts(sort=True)
        # X  Y
        # X  Y
        # A   1    1
        #     3    1
        # B  -2    1
        #    -1    2
        #     4    1
        # Name: count, dtype: int64
        #
        # 3. groupby(sort=False).value_counts(sort=True)
        #   Sort on frequencies.
        # >>> df.groupby("X", sort=False).value_counts(sort=True)
        # X  Y
        # B  -1    2
        #     4    1
        # A   1    1
        #     3    1
        # B  -2    1
        # Name: count, dtype: int64
        #
        # 4. groupby(sort=False).value_counts(sort=False)
        #   Sort on nothing (entries match the order of the original frame).
        # X  Y
        # B   4    1
        # A   1    1
        #     3    1
        # B  -2    1
        #    -1    2
        # Name: count, dtype: int64
        #
        # Lastly, when `normalize` is set with groupby(sort=False).value_counts(sort=True, normalize=True),
        # pandas will sort by the pre-normalization counts rather than the resulting proportions. As this
        # is an uncommon edge case, we cannot handle this using existing QC methods efficiently, so we just
        # update our testing code to account for this.
        # See comment on issue: https://github.com/pandas-dev/pandas/issues/59307#issuecomment-2313767856
        sort_cols = []
        if groupby_sort:
            # When groupby(sort=True), sort the result on the grouping columns
            sort_cols = by
        ascending_cols = [True] * len(sort_cols)
        if sort:
            # When sort=True, also sort on the count/proportion column (always the last)
            sort_cols.append(
                result._modin_frame.data_column_pandas_labels[-1],
            )
            ascending_cols.append(ascending)
        if groupby_sort:
            # When groupby_sort=True, also sort by the non-grouping columns before sorting by
            # the count/proportion column. The left-most column (nearest to the grouping columns
            # is sorted on last).
            # Exclude the grouping columns (always the first) from the sort.
            if as_index:
                # When as_index is true, the non-grouping columns are part of the index columns
                columns_to_filter = result._modin_frame.index_column_pandas_labels
            else:
                # When as_index is false, the non-grouping columns are part of the data columns
                columns_to_filter = result._modin_frame.data_column_pandas_labels
            non_grouping_cols = [
                col_label for col_label in columns_to_filter if col_label not in by
            ]
            sort_cols.extend(non_grouping_cols)
            ascending_cols.extend([True] * len(non_grouping_cols))
        return result.sort_rows_by_column_values(
            columns=sort_cols,
            ascending=ascending_cols,
            kind="stable",
            na_position="last",
            ignore_index=not as_index,  # When as_index=False, take the default positional index
        )

    def groupby_fillna(
        self,
        by: Any,
        axis: int,
        groupby_kwargs: dict[str, Any],
        value: Optional[
            Union[Scalar, Hashable, Mapping, "pd.DataFrame", "pd.Series"]
        ] = None,
        method: Optional[FillnaOptions] = None,
        fill_axis: Optional[int] = None,
        inplace: bool = False,
        limit: Optional[int] = None,
        downcast: Optional[dict] = None,
        drop_data_by_columns: bool = True,
    ) -> "SnowflakeQueryCompiler":
        """
        Replace NaN values using provided method or value.

        Args:
            by: Used to determine the groups for the groupby.
            axis: Group by axis, 0 (index) or 1 (columns), only axis=0 is supported currently.
            groupby_kwargs: Dict[str, Any]
                keyword arguments passed for the groupby.
            value: Optional fill value.
            method: Optional (if no value specified) method of `ffill` or `bfill`.
            fill_axis : Fill axis, 0 (index) or 1 (columns)
            inplace: Not supported
            limit: Maximum number of consecutive NA values to fill.
            downcast: Not supported
            drop_data_by_columns: Internal argument used to determine whether to drop any data columns
                that appear in the "by" list.

        Returns:
            SnowflakeQueryCompiler: with a NaN values using method or value.
        """
        level = groupby_kwargs.get("level", None)

        is_supported = check_is_groupby_supported_by_snowflake(
            by=by, level=level, axis=axis
        )
        if not is_supported:
            ErrorMessage.not_implemented(
                f"Snowpark pandas GroupBy.fillna {_GROUPBY_UNSUPPORTED_GROUPING_MESSAGE}"
            )

        if by is not None and not is_list_like(by):
            by = [by]

        if value is not None and method is not None:
            raise ValueError("Cannot specify both 'value' and 'method'.")

        if value is None and method is None:
            raise ValueError("Must specify a fill 'value' or 'method'.")

        if method is not None and method not in ["ffill", "bfill"]:
            raise ValueError(
                f"Invalid fill method. Expecting pad (ffill) or backfill (bfill). Got {method}"
            )

        if downcast:
            ErrorMessage.not_implemented(
                "Snowpark pandas fillna API doesn't yet support 'downcast' parameter"
            )

        if fill_axis is None:
            fill_axis = 0

        if level is not None:
            query_compiler, by = resample_and_extract_groupby_column_pandas_labels(
                self, by, level
            )
        else:
            query_compiler = self

        frame = query_compiler._modin_frame

        data_column_group_keys = [
            pandas_label
            for pandas_label in frame.data_column_pandas_labels
            if pandas_label in by
        ]

        data_column_group_keys_mask = [
            pandas_label in data_column_group_keys
            for pandas_label in frame.data_column_pandas_labels
        ]

        by_list_snowflake_quoted_identifiers: list[str]

        # If any of the groupby values are None, then there is no fill so check through an expression.
        def groupby_null_expr(
            col_expr: SnowparkColumn,
            col_snowflake_quoted_identifier: str,
            by_list_snowflake_quoted_identifiers: List[str],
        ) -> SnowparkColumn:
            return iff(
                reduce(
                    lambda b1, b2: b1 & b2,
                    [
                        (col(snowflake_quoted_identifier).is_not_null())
                        for snowflake_quoted_identifier in by_list_snowflake_quoted_identifiers
                    ],
                ),
                col_expr,
                pandas_lit(None),
            ).as_(col_snowflake_quoted_identifier)

        # If no method, then we will use the value instead.
        if method is None:
            # If there's no method, then the fill is same as dataframe.fillna with fill value.  Skip any group by
            # data columns in the fill.
            qc = query_compiler._fillna_with_masking(
                value=value,
                self_is_series=False,
                method=None,
                axis=axis,
                limit=limit,
                downcast=downcast,
                columns_mask=data_column_group_keys_mask,
            )

            frame = qc._modin_frame
            new_snowflake_quoted_identifiers = (
                frame.data_column_snowflake_quoted_identifiers
            )

            # Group by snowflake quoted identifiers
            by_list_snowflake_quoted_identifiers = [
                snowflake_quoted_identifier[0]
                for snowflake_quoted_identifier in frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                    by, include_index=True, include_data=True
                )
                if len(snowflake_quoted_identifier) > 0
            ]

            # Generate new snowflake quoted identifiers for output columns so they don't conflict with existing.
            new_snowflake_quoted_identifiers = (
                frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
                    pandas_labels=frame.data_column_pandas_labels
                )
            )

            # Select the resulting columns excluding group by values which are None.
            select_list = frame.index_column_snowflake_quoted_identifiers + [
                groupby_null_expr(
                    col(snowflake_quoted_identifier),
                    new_snowflake_quoted_identifier,
                    by_list_snowflake_quoted_identifiers,
                )
                for new_snowflake_quoted_identifier, snowflake_quoted_identifier in zip(
                    new_snowflake_quoted_identifiers,
                    frame.data_column_snowflake_quoted_identifiers,
                )
            ]
        else:
            # Group by snowflake quoted identifiers
            by_list_snowflake_quoted_identifiers = [
                snowflake_quoted_identifier[0]
                for snowflake_quoted_identifier in frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                    by, include_index=True, include_data=True
                )
                if len(snowflake_quoted_identifier) > 0
            ]

            # Generate new snowflake quoted identifiers for output columns so they don't conflict with existing.
            new_snowflake_quoted_identifiers = (
                frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
                    pandas_labels=frame.data_column_pandas_labels
                )
            )

            if fill_axis == 0:
                # Fill the groups row-wise with values.
                columns_to_fillna_expr = query_compiler._fill_null_values_in_groupby(
                    method, by_list_snowflake_quoted_identifiers, limit
                )

                select_list = frame.index_column_snowflake_quoted_identifiers + [
                    groupby_null_expr(
                        columns_to_fillna_expr[snowflake_quoted_identifier],
                        new_snowflake_quoted_identifier,
                        by_list_snowflake_quoted_identifiers,
                    )
                    for new_snowflake_quoted_identifier, snowflake_quoted_identifier in zip(
                        new_snowflake_quoted_identifiers,
                        frame.data_column_snowflake_quoted_identifiers,
                    )
                ]

            elif fill_axis == 1:
                # Fill the groups column-wise using coalesce of prior columns depending on method direction.
                coalesce_column_list: list[SnowparkColumn] = []
                select_list = []

                data_column_pairs = list(
                    zip(
                        frame.data_column_snowflake_quoted_identifiers,
                        new_snowflake_quoted_identifiers,
                        data_column_group_keys_mask,
                    )
                )

                # If we are doing a ffill, then the current NaN value will fill in with the values from the left,
                # otherwise if bfill will fill with values from the right.  To simplify the processing, we reverse
                # the column order for bfill so it's the same pass to generate the coalece of prior columns, and
                # then reverse again afterwards.
                if method == "bfill":
                    data_column_pairs.reverse()

                for (
                    snowflake_quoted_identifier,
                    new_snowflake_quoted_identifier,
                    is_data_column_group_key,
                ) in data_column_pairs:
                    if is_data_column_group_key:
                        select_list.append(
                            col(snowflake_quoted_identifier).as_(
                                new_snowflake_quoted_identifier
                            )
                        )
                        continue

                    if len(coalesce_column_list) == 0:
                        select_item = col(snowflake_quoted_identifier)
                    else:
                        coalesce_expr = [
                            snowflake_quoted_identifier
                        ] + coalesce_column_list
                        select_item = coalesce(*coalesce_expr)

                    select_item = groupby_null_expr(
                        select_item,
                        new_snowflake_quoted_identifier,
                        by_list_snowflake_quoted_identifiers,
                    )

                    select_list.append(select_item)
                    coalesce_column_list.insert(0, col(snowflake_quoted_identifier))
                    if limit is not None and len(coalesce_column_list) > limit:
                        del coalesce_column_list[-1]

                if method == "bfill":
                    select_list.reverse()

                select_list = (
                    frame.index_column_snowflake_quoted_identifiers + select_list
                )

        new_ordered_dataframe = frame.ordered_dataframe.select(select_list)

        # If any group-by keys were original data (not index) columns, then we drop them in the final result.
        #
        #     The methods ffill, bfill, pad and backfill of DataFrameGroupBy previously included the group labels in
        #      the return value, which was inconsistent with other groupby transforms. Now only the filled values
        #      are returned. (GH 21521)
        if drop_data_by_columns and len(data_column_group_keys) > 0:
            data_column_pandas_labels, data_column_snowflake_quoted_identifiers = zip(
                *[
                    (pandas_label, snowflake_quoted_identifier)
                    for pandas_label, snowflake_quoted_identifier in zip(
                        frame.data_column_pandas_labels,
                        new_snowflake_quoted_identifiers,
                    )
                    if pandas_label not in data_column_group_keys
                ]
            )
        else:
            data_column_pandas_labels, data_column_snowflake_quoted_identifiers = (
                frame.data_column_pandas_labels,
                new_snowflake_quoted_identifiers,
            )

        new_frame = InternalFrame.create(
            ordered_dataframe=new_ordered_dataframe,
            index_column_pandas_labels=frame.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=frame.index_column_snowflake_quoted_identifiers,
            data_column_pandas_labels=data_column_pandas_labels,
            data_column_snowflake_quoted_identifiers=data_column_snowflake_quoted_identifiers,
            data_column_pandas_index_names=frame.data_column_pandas_index_names,
            data_column_types=None,
            index_column_types=None,
        )

        return SnowflakeQueryCompiler(new_frame)

    def groupby_pct_change(
        self,
        by: Any,
        agg_kwargs: dict[str, Any],
        groupby_kwargs: dict[str, Any],
        is_series_groupby: bool,
        **kwargs: Any,
    ) -> "SnowflakeQueryCompiler":
        periods = agg_kwargs.get("periods", 1)
        fill_method = agg_kwargs.get("fill_method", None)
        limit = agg_kwargs.get("limit", None)
        freq = agg_kwargs.get("freq", None)
        axis = agg_kwargs.get("axis", 0)
        level = groupby_kwargs.get("level", None)
        # Per testing, this function does not respect sort/as_index/dropna/group_keys
        is_supported = check_is_groupby_supported_by_snowflake(
            by=by, level=level, axis=axis
        )
        # Remaining parameters are validated in pct_change method
        if not is_supported:
            ErrorMessage.not_implemented(
                f"Snowpark pandas GroupBy.pct_change {_GROUPBY_UNSUPPORTED_GROUPING_MESSAGE}"
            )

        by_labels = by
        if by is not None and not is_list_like(by):
            by_labels = [by]

        if level is not None:
            qc, by_labels = resample_and_extract_groupby_column_pandas_labels(
                self, by, level
            )
        else:
            qc = self

        # Perform fillna before pct_change to account for filling within the group.
        if fill_method is not None:
            qc = qc.groupby_fillna(
                by=by,
                axis=axis,
                groupby_kwargs=groupby_kwargs,
                method=fill_method,
                drop_data_by_columns=False,
            )

        return qc.pct_change(
            periods=periods,
            fill_method=None,  # fillna was already done explicitly to account for the groupby
            limit=limit,
            freq=freq,
            axis=axis,
            by_labels=by_labels,
            # Exclude the `by` columns from the result if this is a DF groupby where labels
            # were explicitly specified, and not generated from a multi-index level.
            drop_by_labels=not is_series_groupby and level is None,
        )

    def get_dummies(
        self,
        prefix: Optional[Union[Hashable, list[Hashable]]],
        prefix_sep: str = "_",
        dummy_na: bool = False,
        columns: Optional[Union[Hashable, list[Hashable]]] = None,
        drop_first: bool = False,
        dtype: Optional[npt.DTypeLike] = None,
        is_series: bool = False,
    ) -> "SnowflakeQueryCompiler":

        """
        Implement one-hot encoding.
        Args:
            prefix: String to append to newly generated column names.
            prefi_sep: Separator between prefix and column name.
            dummy_na: Add a column for nulls.
            columns: Columns to pivot on.
            drop_first: drop the first value.
            dtype: Type of resulting columns.
        Returns:
            A new SnowflakeQueryCompiler instance after applying the get_dummies operation.
        Examples:
        s = pd.Series(list('abca'))
        pd.get_dummies(s)
           a  b  c
        0  1  0  0
        1  0  1  0
        2  0  0  1
        3  1  0  0
        df = pd.DataFrame({'A':['a','b','a'], 'B':['b', 'a', 'c'], 'C':[1, 2, 3]})
        pd.get_dummies(df, prefix=['col1', 'col2'])
        C  col1_a  col1_b  col2_a  col2_b  col2_c
        0  1       1       0       0       1       0
        1  2       0       1       1       0       0
        2  3       1       0       0       0       1
        """
        self._raise_not_implemented_error_for_timedelta()

        if dummy_na is True or drop_first is True or dtype is not None:
            ErrorMessage.not_implemented(
                "get_dummies with non-default dummy_na, drop_first, and dtype parameters"
                + " is not supported yet in Snowpark pandas."
            )
        if columns is None:
            data_types = self.dtypes
            columns = [
                col_name
                for (col_index, col_name) in enumerate(
                    self._modin_frame.data_column_pandas_labels
                )
                if is_series or is_string_dtype(data_types.iloc[col_index])
            ]

        if not isinstance(columns, list):
            columns = [columns]

        # TODO: SNOW-1006947 enable support for get_dummies on columns of non-string types.
        for col_name in self._modin_frame.data_column_pandas_labels:
            if col_name in columns and not is_string_dtype(self.dtypes[col_name]):
                ErrorMessage.not_implemented(
                    "get_dummies with non-string columns parameter"
                    + " is not supported yet in Snowpark pandas."
                )

        if prefix is None and not is_series:
            prefix = columns

        if not isinstance(prefix, list):
            prefix = [prefix]

        if not is_series:
            if len(prefix) != len(columns):
                raise ValueError(
                    f"Length of 'prefix' ({len(prefix)}) did not match the length of the columns being encoded ({len(columns)})."
                )

        if prefix_sep is None:
            prefix_sep = "_"

        result_internal_frame = get_dummies_utils.get_dummies_helper(
            internal_frame=self._modin_frame,
            columns=columns,
            prefixes=prefix,
            prefix_sep=prefix_sep,
        )
        query_compiler = SnowflakeQueryCompiler(result_internal_frame)

        return query_compiler

    def agg(
        self,
        func: AggFuncType,
        axis: int,
        args: Any,
        kwargs: dict[str, Any],
    ) -> "SnowflakeQueryCompiler":
        """
        Aggregate using one or more operations over the specified axis.

        Args:
            func: callable, str, list or dict.
                The aggregation functions to apply on
            axis : 0 (index), 1 (columns)
            args: the arguments passed for the aggregation
            kwargs: keyword arguments passed for the aggregation function.
        """
        numeric_only = kwargs.get("numeric_only", False)
        # Call fallback if the aggregation function passed in the arg is currently not supported
        # by snowflake engine.
        # If we are using Named Aggregations, we need to do our supported check slightly differently.
        uses_named_aggs = using_named_aggregations_for_func(func)
        (
            is_supported,
            unsupported_arguments,
            is_supported_kwargs,
        ) = check_is_aggregation_supported_in_snowflake(
            func, kwargs, axis, _is_df_agg=True
        )
        if not is_supported:
            raise AttributeError(
                f"{repr_aggregate_function(unsupported_arguments, is_supported_kwargs)} is not a valid function for 'Series' object"
            )

        query_compiler = self
        initial_attrs = self._attrs
        if numeric_only:
            # drop off the non-numeric data columns if the data column if numeric_only is configured to be True
            query_compiler = drop_non_numeric_data_columns(
                query_compiler, pandas_labels_for_columns_to_exclude=[]
            )

        if len(query_compiler.columns) == 0:
            return pd.Series()._query_compiler

        internal_frame = query_compiler._modin_frame

        single_agg_func_query_compilers = []
        # If every row specified in the dict has only a single aggregation, which was not provided
        # as a list, then all aggregations should be coalesced together into a single column.
        #
        # This is illustrated by the difference between these two calls. In the first,
        # the members of the dictionary are scalar, so the result should be a series with
        # unnamed columns. In the second, one member was specified as a 1-item list, so
        # the result should have separate columns for each aggregation function as usual.
        # >>> pd.DataFrame({"a": [0, 1], "b": [2, 3]}).agg({1: "max", 0: "min"}, axis=1)
        # 1    3
        # 0    0
        # dtype: int64
        # >>> pd.DataFrame({"a": [0, 1], "b": [2, 3]}).agg({1: "max", 0: ["min"]}, axis=1)
        #    max  min
        # 1  3.0  NaN
        # 0  NaN  0.0
        # should_squeeze cannot be True if we are using named aggregations, since
        # the values for func in that case are either NamedTuples (AggFuncWithLabels) or
        # lists of NamedTuples, both of which are list like.
        should_squeeze = is_dict_like(func) and all(
            not is_list_like(value) for value in func.values()
        )
        if axis == 1:
            if any(
                isinstance(t, TimedeltaType)
                for t in internal_frame.snowflake_quoted_identifier_to_snowpark_pandas_type.values()
            ):
                ErrorMessage.not_implemented_for_timedelta("agg(axis=1)")
            if self.is_multiindex():
                # TODO SNOW-1010307 fix axis=1 behavior with MultiIndex
                ErrorMessage.not_implemented(
                    "axis=1 aggregations with MultiIndex are not yet supported"
                )
            data_col_identifiers = (
                internal_frame.data_column_snowflake_quoted_identifiers
            )

            if is_dict_like(func):
                # This branch is taken if `func` is a dict.
                # For example, suppose we're computing
                # `pd.DataFrame({"a": [0, 1], "b": [2, 3]}).agg({1: ["max"], 0: ["min", "max"]})`
                # where the output should be
                #    max  min
                # 1  3.0  NaN
                # 0  2.0  0.0
                #
                # The element at row label 1/column "min" is NaN because the `min` aggregation was
                # not specified for that row label.
                agg_funcs = [
                    (
                        get_frame_by_row_label(
                            internal_frame=self._modin_frame, key=(row_label,)
                        ),
                        fn if is_list_like(fn) else [fn],
                    )
                    for row_label, fn in func.items()
                ]
            else:
                # If `func` is a scalar or list, every specified aggregation is applied to every row
                # in the frame without the need to union_all later.
                # It is possible for the result to have only one column but return a DF rather than Series
                # (as in `df.min(["min"], axis=1)`). This case is handled by the frontend.
                agg_funcs = [(internal_frame, func if is_list_like(func) else [func])]

            # If `func` is a dict, apply the specified aggregation functions to each row.
            # For every row label specified in the `func` dict, we call the specified aggregation
            # functions to produce a 1xN frame.
            # We concat the resulting aggregations for each row together.
            #
            # If `func` is a scalar or list, then all aggregation functions are applied to every
            # row. In this case, `agg_funcs` should have exactly one element in it.
            for frame, agg_args in agg_funcs:
                agg_col_map = {
                    MODIN_UNNAMED_SERIES_LABEL
                    if should_squeeze
                    else get_pandas_aggr_func_name(
                        agg_arg
                    ): _columns_coalescing_idxmax_idxmin_helper(
                        *(col(c) for c in data_col_identifiers),
                        axis=1,
                        func=agg_arg,
                        keepna=not kwargs.get("skipna", True),
                        pandas_column_labels=frame.data_column_pandas_labels,
                    )
                    if agg_arg in ("idxmin", "idxmax")
                    else get_snowflake_agg_func(
                        agg_arg, kwargs, axis=1
                    ).snowpark_aggregation(*(col(c) for c in data_col_identifiers))
                    for agg_arg in agg_args
                }
                pandas_labels = list(agg_col_map.keys())
                if self.is_multiindex(axis=1):
                    pandas_labels = [
                        (label,) * len(self.columns.names) for label in pandas_labels
                    ]
                single_agg_func_query_compilers.append(
                    SnowflakeQueryCompiler(
                        frame.project_columns(pandas_labels, list(agg_col_map.values()))
                    )
                )
        else:  # axis == 0
            # get a map between the Snowpark pandas column to the aggregation function needs to be applied on the column
            column_to_agg_func = convert_agg_func_arg_to_col_agg_func_map(
                internal_frame,
                func,
                pandas_labels_for_columns_to_exclude_when_agg_on_all=[],
            )

            # generate the quoted identifier for the aggregation function name column
            agg_name_col_quoted_identifier = (
                internal_frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
                    pandas_labels=[AGG_NAME_COL_LABEL],
                )[0]
            )

            def generate_agg_qc(
                col_single_agg_func_map: dict[
                    PandasLabelToSnowflakeIdentifierPair,
                    AggFuncInfo,
                ],
                index_value: str,
            ) -> SnowflakeQueryCompiler:
                """
                Helper function that generates a one-row QC of aggregations determined by
                `col_single_agg_func_map`.

                Parameters
                ----------
                col_single_agg_func_map: Dict[PandasLabelToSnowflakeIdentifierPair, AggFuncInfo]
                    A map of pandas label/column identifier pairs -> AggFuncInfo. This represents
                    the aggregation function to apply to every column (see notes for more details).
                index_value: str
                    The value of the index column. This should always be either MODIN_UNNAMED_SERIES_LABEL or
                    the name of the aggregation function.

                Returns
                -------
                SnowflakeQueryCompiler
                    A 1-row query compiler representing the result of applying the specified aggregations.

                Notes
                -----
                `col_single_agg_func_map` may sometimes contain only a single aggregation function, as when
                `df.agg({"A": ["min"], "B": ["min", "max"]})` is called. In this case, the resulting
                frame should have one row for `min` and one row for `max`, so this function will be
                called twice. It is called first with `col_single_agg_func_map = {"A": min, "B": min}`
                and `index_value = "min"`. This will return a QC representing this frame:
                -----------------
                | INDEX | a | b |
                -----------------
                |   min | 1 | 2 |
                -----------------
                This helper is then called a second time with
                `col_single_agg_func_map = {"A": <dummy>, "B": max}` and `index_value = "max"`
                to produce a QC representing this frame:
                -----------------
                | INDEX | a | b |
                -----------------
                |   max |nan| 8 |
                -----------------
                These two rows are then concatenated together.

                `col_single_agg_func_map` may also contain multiple distinct aggregations when
                `should_squeeze` is True. In this case, the result should contain only a single row,
                so if different aggregations are specified like in `df.agg({"A": min, "B": max})`,
                this function is only called once with `col_single_agg_func_map = {"A": min, "B": max"}
                and `index_value = MODIN_UNNAMED_SERIES_LABEL`. This returns the following:
                -----------------------
                |    INDEX    | a | b |
                -----------------------
                | __reduced__ | 1 | 8 |
                -----------------------
                """
                (col_agg_infos, _) = generate_column_agg_info(
                    internal_frame,
                    col_single_agg_func_map,
                    kwargs,
                    include_agg_func_only_in_result_label=False,
                )
                single_agg_ordered_dataframe = aggregate_with_ordered_dataframe(
                    ordered_dataframe=internal_frame.ordered_dataframe,
                    agg_col_ops=col_agg_infos,
                    agg_kwargs=kwargs,
                    index_column_snowflake_quoted_identifier=internal_frame.index_column_snowflake_quoted_identifiers,
                )
                # append an extra column with the name of the aggregation function
                single_agg_ordered_dataframe = append_columns(
                    single_agg_ordered_dataframe,
                    agg_name_col_quoted_identifier,
                    pandas_lit(index_value),
                )
                single_agg_ordered_dataframe = single_agg_ordered_dataframe.sort(
                    OrderingColumn(agg_name_col_quoted_identifier)
                )
                single_agg_dataframe = InternalFrame.create(
                    ordered_dataframe=single_agg_ordered_dataframe,
                    data_column_pandas_labels=[
                        col.agg_pandas_label for col in col_agg_infos
                    ],
                    data_column_pandas_index_names=internal_frame.data_column_pandas_index_names,
                    data_column_snowflake_quoted_identifiers=[
                        col.agg_snowflake_quoted_identifier for col in col_agg_infos
                    ],
                    index_column_pandas_labels=[None],
                    index_column_snowflake_quoted_identifiers=[
                        agg_name_col_quoted_identifier
                    ],
                    data_column_types=[
                        col.data_type
                        if isinstance(col.data_type, SnowparkPandasType)
                        and col.snowflake_agg_func.preserves_snowpark_pandas_types
                        else None
                        for col in col_agg_infos
                    ],
                    index_column_types=None,
                )
                return SnowflakeQueryCompiler(single_agg_dataframe)

            if should_squeeze:
                # Return a single 1-row frame.
                # This branch is taken if `func` is a dict where all values are scalar function/str.
                # We cannot use `agg_func_to_col_map` here because when `should_squeeze` is true
                # we need all aggregations to be in a single row, whereas in all other cases
                # we have one QC for each aggregation that we can UNION ALL together later.
                # We cannot UNION ALL in the `should_squeeze` case because the result must always
                # have exactly one row.
                # For example, suppose we call `df.agg({"a": min, "b": max}, axis=0)`. Here,
                # `should_squeeze` is true, and we should produce the following 1-row frame:
                # ---------------------------
                # |       INDEX |   a |   b |
                # ---------------------------
                # | __reduced__ |   1 |   8 |
                # ---------------------------
                #
                # However, if we were to share logic with the non-`should_squeeze` case, we would
                # produce the following two frames:
                # ---------------------           ---------------------
                # | INDEX |   a |   b |           | INDEX |   a |   b |
                # --------------------- UNION ALL ---------------------
                # |   min |   1 | nan |           |   max | nan |   8 |
                # ---------------------           ---------------------
                # Since the result of the UNION ALL will have 2 rows (with NaN values filled in)
                # we cannot use the result in the should_squeeze case.
                col_single_agg_func_map = {
                    col: AggFuncInfo(func=agg_func, is_dummy_agg=False)
                    for col, agg_func in column_to_agg_func.items()
                }

                single_agg_func_query_compilers.append(
                    generate_agg_qc(col_single_agg_func_map, MODIN_UNNAMED_SERIES_LABEL)
                )

            else:
                if uses_named_aggs:
                    # If this is true, then we are dealing with agg with NamedAggregations.

                    # When we have multiple columns with the same pandas label, we need to
                    # generate dummy aggregations over all of the columns, as otherwise,
                    # when we union the QueryCompilers, the duplicate columns will stack up
                    # into one column. Example:
                    #    A  A
                    # 0  0  1
                    # 1  2  3
                    # If we call `df.agg(x=("A", "max"))` on the above DataFrame, we expect:
                    #      A    A
                    # x  2.0  NaN
                    # x  NaN  3.0
                    # but without the dummy aggregations, we get QC's that correspond to the following
                    # frames:
                    #      A
                    # x  2.0
                    # and
                    #      A
                    # x  3.0
                    # so when we concatenate, we get a result that looks like this:
                    #      A
                    # x  2.0
                    # x  3.0
                    # which is wrong. Adding dummy aggregations means that our individual QCs will look like
                    #      A    A
                    # x  2.0  NaN
                    # and
                    #      A    A
                    # x  NaN  3.0
                    # so concatenation will give us the correct result.
                    # We first check if it is the case that we are aggregating over multiple columns with the same pandas label
                    has_col_with_duplicate_pandas_label = len(
                        {id_pair.pandas_label for id_pair in column_to_agg_func.keys()}
                    ) < len(column_to_agg_func.keys())

                    def generate_single_agg_column_func_map(
                        identifier_pair: PandasLabelToSnowflakeIdentifierPair,
                        agg_func: AggFuncWithLabel,
                    ) -> dict[PandasLabelToSnowflakeIdentifierPair, AggFuncInfo]:
                        """
                        Helper function to produce the agg func map for a single aggregation, including dummy aggregations.

                        Notes:
                        Adds dummy aggregations for all columns that will be aggregated over.
                        """
                        col_single_agg_func_map = {}
                        for c in column_to_agg_func.keys():
                            if c == identifier_pair:
                                col_single_agg_func_map[c] = AggFuncInfo(
                                    func=agg_func.func, is_dummy_agg=False
                                )
                            else:
                                col_single_agg_func_map[c] = AggFuncInfo(
                                    func="min", is_dummy_agg=True
                                )
                        return col_single_agg_func_map

                    # We can't simply append the generated_qcs to single_agg_func_query_compilers, because
                    # we have changed the order of the aggregations - see the comment in the function
                    # extract_validate_and_try_convert_named_aggs_from_kwargs for an explanation of how
                    # and why the order changes. Instead, we get a mapping of the name of the aggregation to the
                    # QueryCompiler it produces, which we can then use to insert into single_agg_func_query_compilers
                    # in the correct order, which we recieve from the frontend, so that when the concatenation occurs,
                    # the final QueryCompiler is ordered correctly.
                    index_label_to_generated_qcs: dict[
                        Hashable, list["SnowflakeQueryCompiler"]
                    ] = {}
                    for (
                        identifier_pair,
                        agg_func_with_label,
                    ) in column_to_agg_func.items():
                        if not isinstance(agg_func_with_label, list):
                            agg_func_with_label = [agg_func_with_label]
                        for agg_func in agg_func_with_label:
                            if not has_col_with_duplicate_pandas_label:
                                agg_func_map = {
                                    identifier_pair: AggFuncInfo(
                                        agg_func.func, is_dummy_agg=False
                                    )
                                }
                            else:
                                agg_func_map = generate_single_agg_column_func_map(
                                    identifier_pair, agg_func
                                )

                            new_qc = generate_agg_qc(
                                agg_func_map,
                                agg_func.pandas_label,
                            )
                            index_label_to_generated_qcs[
                                agg_func.pandas_label
                            ] = index_label_to_generated_qcs.get(
                                agg_func.pandas_label, []
                            ) + [
                                new_qc
                            ]
                    correct_order_of_index_labels = list(kwargs.keys())
                    for index_label in correct_order_of_index_labels:
                        single_agg_func_query_compilers.extend(
                            index_label_to_generated_qcs[index_label]
                        )
                else:
                    # get a map between each aggregation function and the columns needs to apply this aggregation function
                    agg_func_to_col_map = get_agg_func_to_col_map(column_to_agg_func)

                    # aggregation creates an index column with the aggregation function names as its values
                    # For example: with following dataframe
                    #       A   B   C
                    #   0   1   2   3
                    #   1   4   5   6
                    #   2   7   8   9
                    # after we call df.aggregate({"A": ["min"], "B": ["max"]}), the result is following
                    #       A   B
                    # min   1   NaN
                    # max   NaN	8
                    #
                    # However, if all values in the agg_func dict are scalar strings/functions rather than lists,
                    # then the result will instead be a Series:
                    # >>> df.aggregate({"A": "min", "B": "max"})
                    # 0    1
                    # 1    8
                    # dtype: int64
                    for agg_func, cols in agg_func_to_col_map.items():
                        col_single_agg_func_map = {
                            column: AggFuncInfo(
                                func=agg_func if column in cols else "min",
                                is_dummy_agg=column not in cols,
                            )
                            for column in column_to_agg_func.keys()
                        }
                        single_agg_func_query_compilers.append(
                            generate_agg_qc(
                                col_single_agg_func_map,
                                get_pandas_aggr_func_name(agg_func),
                            )
                        )

        assert single_agg_func_query_compilers, "no aggregation result"
        if len(single_agg_func_query_compilers) == 1:
            result = single_agg_func_query_compilers[0]
        else:
            result = single_agg_func_query_compilers[0].concat(
                axis=0, other=single_agg_func_query_compilers[1:]
            )

        if axis == 0 and (should_squeeze or is_scalar(func) or callable(func)):
            # In this branch, the concatenated frame is a 1-row frame, but needs to be converted
            # into a 1-column frame so the frontend can wrap it as a Series
            result = result.transpose_single_row()
            # Set the single column's name to MODIN_UNNAMED_SERIES_LABEL
            result = result.set_columns([MODIN_UNNAMED_SERIES_LABEL])
        # native pandas clears attrs if the aggregation was a list, but propagates it otherwise
        if is_list_like(func):
            result._attrs = {}
        else:
            result._attrs = copy.deepcopy(initial_attrs)
        return result

    def insert(
        self,
        loc: int,
        pandas_label: Hashable,
        value: Union[Scalar, "SnowflakeQueryCompiler"],
        join_on_index: Optional[bool] = False,
        replace: bool = False,
    ) -> "SnowflakeQueryCompiler":
        """
        Insert new column at specified location.

        Args:
            loc: Insertion index, must be 0 <= loc <= len(columns)
            pandas_label: Label for the inserted column.
            value: Value of the column. Can be Scalar or SnowflakeQueryCompiler with one column.
            join_on_index: If True, join 'value' query compiler with index of
              this query compiler. If False, join on row position.
            replace: If True, new column is not appended but new column replaces existing column at loc
        Returns:
            A new SnowflakeQueryCompiler instance with new column.
        """
        if not isinstance(value, SnowflakeQueryCompiler):
            # Scalar value
            new_internal_frame = self._modin_frame.append_column(
                pandas_label, pandas_lit(value)
            )
        elif join_on_index:
            assert len(value.columns) == 1

            # rename given Series (as SnowflakeQueryCompiler) to the desired label
            value = value.set_columns([pandas_label])

            if (
                self._modin_frame.num_index_columns
                == value._modin_frame.num_index_columns
            ):
                # In Native pandas Number of rows should remain unchanged, and therefore one-to-many
                # join is disallowed, and a ValueError with message "cannot reindex on an axis with duplicate labels"
                # is raised when the value index contains duplication. For example: with the following frame
                #       A       B
                # 1     1       2
                # 2     3       2
                # 3     4       3
                # and the value frame
                # 1  0
                # 2  0
                # 2  3
                # frame.insert(2, "C", value) raises ValueError.

                # However, In Snowpark pandas, to avoid eager evaluation, we do not perform the uniqueness check.
                # Therefore, the above example will not raise error anymore, instead, it produces a result with left
                # align behavior, and produces result like following:
                #       A       B       C
                # 1     1       2       0
                # 2     3       2       0
                # 2     3       2       3
                # 3     4       3       NaN

                # set the index name of the value frame to be the same as the frame to allow join on all index columns
                new_value = value.set_index_names(
                    self._modin_frame.index_column_pandas_labels
                )
                # Left align on index columns.
                new_internal_frame, _ = join_utils.align_on_index(
                    self._modin_frame,
                    new_value._modin_frame,
                    how="coalesce",
                )
            else:
                # We raise error when number of index columns in 'value' are different
                # from number of index columns in 'self'.
                # This behavior is differs from native pandas in following cases
                # 1. self.index.nlevels > value.index.nlevles: Native pandas will insert
                #    new column with all null values.
                # 2. self.index.nlevels < value.index.nlevles and self is empty: Native
                #    pandas will use 'value' as final result.
                raise ValueError(
                    "Number of index levels of inserted column are different from frame index"
                )
        else:
            # rename given Series (as SnowflakeQueryCompiler) to the desired label
            value = value.set_columns([pandas_label])
            self_frame = self._modin_frame.ensure_row_position_column()
            value_frame = value._modin_frame.ensure_row_position_column()

            new_internal_frame = join_utils.align(
                left=self_frame,
                right=value_frame,
                left_on=[self_frame.row_position_snowflake_quoted_identifier],
                right_on=[value_frame.row_position_snowflake_quoted_identifier],
                how="coalesce",
            ).result_frame

        # New column is added at the end. Move this to desired location as specified by
        # 'loc'
        def move_last_element(arr: list, index: int) -> None:
            if replace:
                # swap element at loc with new column at end, then drop last element
                arr[index], arr[-1] = arr[-1], arr[index]
                arr.pop()
            else:
                # move last element to desired location
                last_element = arr.pop()
                arr.insert(index, last_element)

        data_column_pandas_labels = new_internal_frame.data_column_pandas_labels
        move_last_element(data_column_pandas_labels, loc)
        data_column_snowflake_quoted_identifiers = (
            new_internal_frame.data_column_snowflake_quoted_identifiers
        )
        data_column_types = new_internal_frame.cached_data_column_snowpark_pandas_types
        move_last_element(data_column_snowflake_quoted_identifiers, loc)
        move_last_element(data_column_types, loc)

        new_internal_frame = InternalFrame.create(
            ordered_dataframe=new_internal_frame.ordered_dataframe,
            data_column_pandas_labels=data_column_pandas_labels,
            data_column_snowflake_quoted_identifiers=data_column_snowflake_quoted_identifiers,
            data_column_pandas_index_names=new_internal_frame.data_column_pandas_index_names,
            index_column_pandas_labels=new_internal_frame.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=new_internal_frame.index_column_snowflake_quoted_identifiers,
            data_column_types=data_column_types,
            index_column_types=new_internal_frame.cached_index_column_snowpark_pandas_types,
        )
        return SnowflakeQueryCompiler(new_internal_frame)

    def set_index_from_columns(
        self,
        keys: list[Hashable],
        drop: Optional[bool] = True,
        append: Optional[bool] = False,
        include_index: Optional[bool] = True,
    ) -> "SnowflakeQueryCompiler":
        """
        Create or update index (row labels) from a list of columns.

        Args:
            keys: list of hashable
              The list of column names that will become the new index.
            drop: bool, default True
              Whether to drop the columns provided in the `keys` argument.
            append: bool, default False
              Whether to add the columns in `keys` as new levels appended to the
              existing index.
            include_index: bool, default True
              Whether the keys can also include index column lables as well.

        Returns:
            A new QueryCompiler instance with updated index.
        """

        index_column_pandas_labels = keys
        index_column_snowflake_quoted_identifiers = []
        for (
            ids
        ) in self._modin_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
            keys, include_index=include_index
        ):
            # Error checking for missing labels is already done in frontend layer.
            index_column_snowflake_quoted_identifiers.append(ids[0])

        if drop:
            # Exclude 'keys' from data columns.
            data_column_pandas_labels = []
            data_column_snowflake_quoted_identifiers = []
            for i, label in enumerate(self._modin_frame.data_column_pandas_labels):
                if label not in keys:
                    data_column_pandas_labels.append(label)
                    data_column_snowflake_quoted_identifiers.append(
                        self._modin_frame.data_column_snowflake_quoted_identifiers[i]
                    )
        else:
            data_column_pandas_labels = self._modin_frame.data_column_pandas_labels
            data_column_snowflake_quoted_identifiers = (
                self._modin_frame.data_column_snowflake_quoted_identifiers
            )

        id_to_type = (
            self._modin_frame.snowflake_quoted_identifier_to_snowpark_pandas_type
        )
        index_column_snowpark_pandas_types = [
            id_to_type.get(id) for id in index_column_snowflake_quoted_identifiers
        ]
        data_column_snowpark_pandas_types = [
            id_to_type.get(id) for id in data_column_snowflake_quoted_identifiers
        ]

        # Generate aliases for new index columns if
        # 1. 'keys' are also kept as data columns, or
        # 2. 'keys' have duplicates.
        #   For example:
        #     >>> pd.DataFrame({"A": [1], "B": [2]})
        #     >>> pd.set_index(["A", "A"]
        #           B
        #       A A
        #       1 1 2
        # Note: When drop is True and there are no duplicates in 'keys', this is purely
        # a client side metadata operation.
        ordered_dataframe = self._modin_frame.ordered_dataframe
        if not drop or len(set(keys)) != len(keys):
            new_index_identifiers = self._modin_frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
                pandas_labels=keys
            )
            values = [col(sf_id) for sf_id in index_column_snowflake_quoted_identifiers]
            index_column_snowflake_quoted_identifiers = new_index_identifiers
            # Create duplicate identifiers in underlying snowpark dataframe.
            # Generates SQL like 'SELECT old_id as new_id_1, old_id as new_id_2 ...'
            ordered_dataframe = append_columns(
                ordered_dataframe, new_index_identifiers, values
            )

        if append:
            # Append to existing index columns instead of replacing it.
            index_column_pandas_labels = (
                self._modin_frame.index_column_pandas_labels
                + index_column_pandas_labels
            )
            index_column_snowflake_quoted_identifiers = (
                self._modin_frame.index_column_snowflake_quoted_identifiers
                + index_column_snowflake_quoted_identifiers
            )
            index_column_snowpark_pandas_types = (
                self._modin_frame.cached_index_column_snowpark_pandas_types
                + index_column_snowpark_pandas_types
            )

        frame = InternalFrame.create(
            ordered_dataframe=ordered_dataframe,
            index_column_pandas_labels=index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=index_column_snowflake_quoted_identifiers,
            data_column_pandas_index_names=self._modin_frame.data_column_pandas_index_names,
            data_column_pandas_labels=data_column_pandas_labels,
            data_column_snowflake_quoted_identifiers=data_column_snowflake_quoted_identifiers,
            data_column_types=data_column_snowpark_pandas_types,
            index_column_types=index_column_snowpark_pandas_types,
        )
        return SnowflakeQueryCompiler(frame)

    @snowpark_pandas_type_immutable_check
    def rename(
        self,
        *,
        index_renamer: Optional[Renamer] = None,
        columns_renamer: Optional[Renamer] = None,
        # TODO: SNOW-800889 handle level is hashable
        level: Optional[Union[Hashable, int]] = None,
        errors: Optional[IgnoreRaise] = "ignore",
    ) -> "SnowflakeQueryCompiler":
        internal_frame = self._modin_frame
        if index_renamer is not None:
            # rename index means to update the values in the index columns
            # TODO: SNOW-850784 convert all mapper renamer into a Snowpark pandas Series and use insert and coalesce to
            # generate the new index columns in parallel
            if callable(index_renamer):
                # TODO: use df.apply() to handle callable
                ErrorMessage.not_implemented(
                    "Snowpark pandas rename API doesn't yet support callable mapper"
                )
            else:
                # TODO: SNOW-841607 support multiindex in join_utils.join. Now all multiindex cases are not supported.
                if (
                    self._modin_frame.is_multiindex(axis=0)
                    or self._modin_frame.is_multiindex(axis=1)
                    or index_renamer._query_compiler._modin_frame.is_multiindex(axis=0)
                ):
                    ErrorMessage.not_implemented(
                        "Snowpark pandas rename API is not yet supported for multi-index objects"
                    )
                else:
                    index_col_id = (
                        internal_frame.index_column_snowflake_quoted_identifiers[0]
                    )
                    index_renamer_internal_frame = (
                        index_renamer._query_compiler._modin_frame
                    )

                    if errors == "raise":
                        # raise a KeyError when a dict-like mapper, index, or columns contains labels that are not
                        # present in the Index being transformed. Here we use inner join and count on the result to
                        # check whether renamer is valid.
                        label_join_result = join_utils.join(
                            internal_frame,
                            index_renamer_internal_frame,
                            left_on=[index_col_id],
                            right_on=index_renamer_internal_frame.index_column_snowflake_quoted_identifiers,
                            how="inner",
                        ).result_frame
                        if not label_join_result.num_rows:
                            raise KeyError(
                                f"{index_renamer.index.values.tolist()} not found in axis"
                            )

                    # Left join index_renamer_internal_frame.
                    internal_frame, result_column_mapper = join_utils.join(
                        internal_frame,
                        index_renamer_internal_frame,
                        left_on=[index_col_id],
                        right_on=index_renamer_internal_frame.index_column_snowflake_quoted_identifiers,
                        how="left",
                    )
                    # use coalesce to replace index values with the renamed ones
                    new_index_col_id = result_column_mapper.map_right_quoted_identifiers(
                        index_renamer_internal_frame.data_column_snowflake_quoted_identifiers
                    )[
                        0
                    ]
                    # if index datatype may change after rename, we have to cast the new index column to variant
                    quoted_identifier_to_snowflake_type_map = (
                        index_renamer_internal_frame.quoted_identifier_to_snowflake_type()
                    )
                    index_datatype_may_change = [
                        quoted_identifier_to_snowflake_type_map[quoted_identifier]
                        for quoted_identifier in index_renamer_internal_frame.index_column_snowflake_quoted_identifiers
                    ] != [
                        quoted_identifier_to_snowflake_type_map[quoted_identifier]
                        for quoted_identifier in index_renamer_internal_frame.data_column_snowflake_quoted_identifiers
                    ]
                    index_col, new_index_col = col(index_col_id), col(new_index_col_id)
                    if index_datatype_may_change:
                        index_col, new_index_col = cast(index_col, VariantType()), cast(
                            new_index_col, VariantType()
                        )
                    new_index_col = coalesce(new_index_col, index_col)
                    internal_frame = internal_frame.update_snowflake_quoted_identifiers_with_expressions(
                        {index_col_id: new_index_col}
                    ).frame
                    internal_frame = InternalFrame.create(
                        ordered_dataframe=internal_frame.ordered_dataframe,
                        data_column_pandas_labels=internal_frame.data_column_pandas_labels[
                            :-1
                        ],  # remove the last column, i.e., the index renamer column
                        data_column_snowflake_quoted_identifiers=internal_frame.data_column_snowflake_quoted_identifiers[
                            :-1
                        ],
                        # remove the last column, i.e., the index renamer column
                        data_column_pandas_index_names=internal_frame.data_column_pandas_index_names,
                        index_column_pandas_labels=internal_frame.index_column_pandas_labels,
                        index_column_snowflake_quoted_identifiers=internal_frame.index_column_snowflake_quoted_identifiers,
                        data_column_types=internal_frame.cached_data_column_snowpark_pandas_types[
                            :-1
                        ],
                        index_column_types=internal_frame.cached_index_column_snowpark_pandas_types,
                    )

        new_qc = SnowflakeQueryCompiler(internal_frame)
        if columns_renamer is not None:
            # renaming columns needs to change the column names (not values in the columns)
            new_data_column_pandas_labels = (
                native_pd.DataFrame(columns=self.columns)
                .rename(columns=columns_renamer, level=level, errors=errors)
                .columns
            )
            new_qc = new_qc.set_columns(new_data_column_pandas_labels)

        return new_qc

    def dataframe_to_datetime(
        self,
        errors: DateTimeErrorChoices = "raise",
        dayfirst: bool = False,
        yearfirst: bool = False,
        utc: bool = False,
        format: Optional[str] = None,
        exact: Union[bool, lib.NoDefault] = lib.no_default,
        unit: Optional[str] = None,
        infer_datetime_format: Union[lib.NoDefault, bool] = lib.no_default,
        origin: DateTimeOrigin = "unix",
    ) -> "SnowflakeQueryCompiler":
        """
        Convert dataframe to the datetime dtype.

        Args:
            errors: to_datetime errors
            dayfirst: to_datetime dayfirst
            yearfirst: to_datetime yearfirst
            utc: to_datetime utc
            format: to_datetime format
            exact: to_datetime exact
            unit: to_datetime unit
            infer_datetime_format: to_datetime infer_datetime_format
            origin: to_datetime origin
        Returns:
            SnowflakeQueryCompiler:
            QueryCompiler with a single data column converted to datetime dtype.
        """
        raise_if_to_datetime_not_supported(format, exact, infer_datetime_format, origin)
        if origin != "unix":
            """
            Non-default values of the `origin` argument are only valid for scalars and 1D arrays.

            pandas will raise a different error message depending on whether a dict or
            a dataframe-wrapped dict was passed in as argument. This distinction is not
            particularly important for us.

            >>> native_pd.to_datetime({"year": [2000], "month": [3], "day": [1]}, origin=1e9)
            ValueError: '{'year': [2000], 'month': [3], 'day': [1]}' is not compatible with origin='1000000000.0'; it must be numeric with a unit specified
            >>> native_pd.to_datetime(pd.DataFrame({"year": [2000], "month": [3], "day": [1]}), origin=1e9)
            TypeError: arg must be a string, datetime, list, tuple, 1-d array, or Series
            """
            raise TypeError(
                "arg must be a string, datetime, list, tuple, 1-d array, or Series"
            )
        # first check all dataframe column names are valid and make sure required names, i.e, year, month, and, day,
        # are always included. pandas use case insenstive check for those names so we follow the same way.
        # pandas also allows including plural, abbreviated, and unabbreviated forms
        # if the same field is specified multiple times (e.g. "year" and "years" in the same dataframe),
        # pandas simply accepts the last one in iteration order
        str_label_to_id_map = {}
        for label, id in zip(
            self._modin_frame.data_column_pandas_labels,
            self._modin_frame.data_column_snowflake_quoted_identifiers,
        ):
            if (
                not isinstance(label, str)
                or label.lower() not in VALID_TO_DATETIME_DF_KEYS
            ):
                raise ValueError(
                    f"extra keys have been passed to the datetime assemblage: [{str(label)}]"
                )
            str_label_to_id_map[VALID_TO_DATETIME_DF_KEYS[label.lower()]] = id
        missing_required_labels = []
        for label in ["day", "month", "year"]:
            if label not in str_label_to_id_map:
                missing_required_labels.append(label)
        if missing_required_labels:
            raise ValueError(
                f"to assemble mappings requires at least that [year, month, day] be specified: [{','.join(missing_required_labels)}] is missing"
            )

        id_to_sf_type_map = self._modin_frame.quoted_identifier_to_snowflake_type()
        # Raise error if the original data type is not integer. Note pandas will always cast other types to integer and
        # the way it does is not quite straightforward to implement. For example, a month value 3.1 will be cast to
        # March with 10 days and the 10 days will be added with what values in the day column.
        for sf_type in id_to_sf_type_map.values():
            if not isinstance(sf_type, _IntegralType):
                ErrorMessage.not_implemented(
                    "Snowpark pandas to_datetime API doesn't yet support non integer types"
                )
        # if the column is already integer, we can use Snowflake timestamp_ntz_from_parts function to handle it
        # since timestamp_ntz_from_parts only allows nanosecond as the fraction input, we generate it from the
        # input columns
        nanosecond = pandas_lit(0)
        if "ms" in str_label_to_id_map:
            nanosecond += col(str_label_to_id_map["ms"]) * 10**6
        if "us" in str_label_to_id_map:
            nanosecond += col(str_label_to_id_map["us"]) * 10**3
        if "ns" in str_label_to_id_map:
            nanosecond += col(str_label_to_id_map["ns"])
        new_column_name = (
            self._modin_frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
                pandas_labels=["timestamp_ntz_from_parts"],
            )[0]
        )
        new_column = timestamp_ntz_from_parts(
            str_label_to_id_map["year"],
            str_label_to_id_map["month"],
            str_label_to_id_map["day"],
            str_label_to_id_map["hour"] if "hour" in str_label_to_id_map else 0,
            str_label_to_id_map["minute"] if "minute" in str_label_to_id_map else 0,
            str_label_to_id_map["second"] if "second" in str_label_to_id_map else 0,
            nanosecond,
        ).as_(new_column_name)
        # new selected columns will add the timestamp_ntz_from_parts column as the only data column. Here, we make
        # sure exclude existing data columns
        new_selected_columns = set(
            [new_column]
            + self._modin_frame.ordering_column_snowflake_quoted_identifiers
            + [self._modin_frame.row_position_snowflake_quoted_identifier]
            + self._modin_frame.index_column_snowflake_quoted_identifiers
        )

        new_dataframe = self._modin_frame.ordered_dataframe.select(new_selected_columns)
        return SnowflakeQueryCompiler(
            InternalFrame.create(
                ordered_dataframe=new_dataframe,
                data_column_pandas_labels=[MODIN_UNNAMED_SERIES_LABEL],
                data_column_snowflake_quoted_identifiers=[new_column_name],
                data_column_pandas_index_names=self._modin_frame.data_column_pandas_index_names,
                index_column_pandas_labels=self._modin_frame.index_column_pandas_labels,
                index_column_snowflake_quoted_identifiers=self._modin_frame.index_column_snowflake_quoted_identifiers,
                data_column_types=[None],
                index_column_types=[None],
            )
        )

    def to_timedelta(
        self,
        unit: str = "ns",
        errors: DateTimeErrorChoices = "raise",
        include_index: bool = False,
    ) -> "SnowflakeQueryCompiler":
        """
        Convert data to timedelta.

        Args:
            unit: Denotes unit of the input data.  Defaults to 'ns'.
                Possible values:
                * 'W'
                * 'D' / 'days' / 'day'
                * 'hours' / 'hour' / 'hr' / 'h' / 'H'
                * 'm' / 'minute' / 'min' / 'minutes' / 'T'
                * 's' / 'seconds' / 'sec' / 'second' / 'S'
                * 'ms' / 'milliseconds' / 'millisecond' / 'milli' / 'millis' / 'L'
                * 'us' / 'microseconds' / 'microsecond' / 'micro' / 'micros' / 'U'
                * 'ns' / 'nanoseconds' / 'nano' / 'nanos' / 'nanosecond' / 'N'
            errors : {'ignore', 'raise', 'coerce'}, default 'raise'
                - If 'raise', then invalid parsing will raise an exception.
                - If 'coerce', then invalid parsing will be set as NaT.
                - If 'ignore', then invalid parsing will return the input.
            include_index: If true, also convert index columns to timedelta.

        Returns:
            A new query compiler with the data converted to timedelta.
        """
        if errors != "raise":
            ErrorMessage.parameter_not_implemented_error("errors", "pd.to_timedelta")
        internal_frame = self._modin_frame
        col_ids = internal_frame.data_column_snowflake_quoted_identifiers
        if include_index:
            col_ids.extend(internal_frame.index_column_snowflake_quoted_identifiers)

        # Raise error if the original data type is not numeric.
        id_to_type = internal_frame.quoted_identifier_to_snowflake_type(col_ids)
        for id, sf_type in id_to_type.items():
            if isinstance(sf_type, TimedeltaType):
                # already timedelta
                col_ids.remove(id)
            elif isinstance(sf_type, StringType):
                ErrorMessage.not_implemented(
                    "Snowpark pandas method pd.to_timedelta does not yet support conversion from string type"
                )
            elif not isinstance(sf_type, _NumericType):
                raise TypeError(
                    f"dtype {TypeMapper.to_pandas(sf_type)} cannot be converted to timedelta64[ns]"
                )

        # If all columns are already timedelta. No conversion is needed.
        if not col_ids:
            return self

        return SnowflakeQueryCompiler(
            internal_frame.update_snowflake_quoted_identifiers_with_expressions(
                {col_id: col_to_timedelta(col(col_id), unit) for col_id in col_ids},
                [TimedeltaType()] * len(col_ids),
            ).frame
        )

    def series_to_datetime(
        self,
        errors: DateTimeErrorChoices = "raise",
        dayfirst: bool = False,
        yearfirst: bool = False,
        utc: bool = False,
        format: Optional[str] = None,
        exact: Union[bool, lib.NoDefault] = lib.no_default,
        unit: Optional[str] = None,
        infer_datetime_format: Union[lib.NoDefault, bool] = lib.no_default,
        origin: DateTimeOrigin = "unix",
        include_index: bool = False,
    ) -> "SnowflakeQueryCompiler":
        """
        Convert series to the datetime dtype.

        Args:
            errors: to_datetime errors
            dayfirst: to_datetime dayfirst
            yearfirst: to_datetime yearfirst
            utc: to_datetime utc
            format: to_datetime format
            exact: to_datetime exact
            unit: to_datetime unit
            infer_datetime_format: to_datetime infer_datetime_format
            origin: to_datetime origin
            include_index: If True, also convert index columns to datetime.
        Returns:
            SnowflakeQueryCompiler:
            QueryCompiler with a single data column converted to datetime dtype.
        """
        raise_if_to_datetime_not_supported(format, exact, infer_datetime_format, origin)
        # convert format to sf_format which will be valid to use by to_timestamp functions in Snowflake
        sf_format = (
            to_snowflake_timestamp_format(format) if format is not None else None
        )
        col_ids = []
        if include_index:
            col_ids = self._modin_frame.index_column_snowflake_quoted_identifiers
        col_ids.extend(self._modin_frame.data_column_snowflake_quoted_identifiers)
        id_to_sf_type_map = self._modin_frame.quoted_identifier_to_snowflake_type(
            col_ids
        )
        to_datetime_cols = {}

        for col_id in col_ids:
            sf_type = id_to_sf_type_map[col_id]
            if isinstance(sf_type, BooleanType):
                # bool is not allowed in to_datetime (but note that bool is allowed by astype)
                raise TypeError("dtype bool cannot be converted to datetime64[ns]")
            elif isinstance(sf_type, TimedeltaType):
                raise TypeError(
                    "dtype timedelta64[ns] cannot be converted to datetime64[ns]"
                )

            to_datetime_cols[col_id] = generate_timestamp_col(
                col(col_id),
                sf_type,
                sf_format=sf_format,
                errors=errors,
                target_tz="UTC" if utc else None,
                unit="ns" if unit is None else unit,
                origin=origin,
            )

        return SnowflakeQueryCompiler(
            self._modin_frame.update_snowflake_quoted_identifiers_with_expressions(
                to_datetime_cols
            ).frame
        )

    def concat(
        self,
        axis: Axis,
        other: list["SnowflakeQueryCompiler"],
        *,
        join: Optional[Literal["outer", "inner"]] = "outer",
        ignore_index: bool = False,
        keys: Optional[Sequence[Hashable]] = None,
        levels: Optional[list[Sequence[Hashable]]] = None,
        names: Optional[list[Hashable]] = None,
        verify_integrity: Optional[bool] = False,
        sort: Optional[bool] = False,
    ) -> "SnowflakeQueryCompiler":
        """
        Concatenate `self` with passed query compilers along specified axis.
        Args:
            axis : {0, 1}
              Axis to concatenate along. 0 is for index and 1 is for columns.
            other : SnowflakeQueryCompiler or list of such
              Objects to concatenate with `self`.
            join : {'inner', 'outer'}, default 'outer'
              How to handle indexes on other axis (or axes).
            ignore_index : bool, default False
              If True, do not use the index values along the concatenation axis. The
              resulting axis will be labeled 0, ..., n - 1. This is useful if you are
              concatenating objects where the concatenation axis does not have
              meaningful indexing information. Note the index values on the other
              axes are still respected in the join.
            keys : sequence, default None
              If multiple levels passed, should contain tuples. Construct
              hierarchical index using the passed keys as the outermost level.
            levels : list of sequences, default None
              Specific levels (unique values) to use for constructing a
              MultiIndex. Otherwise they will be inferred from the keys.
            names : list, default None
              Names for the levels in the resulting hierarchical index.
            verify_integrity : bool, default False
              Check whether the new concatenated axis contains duplicates. This can
              be very expensive relative to the actual data concatenation.
            sort : bool, default False
              Sort non-concatenation axis if it is not already aligned when `join`
              is 'outer'.
              This has no effect when ``join='inner'``, which already preserves
              the order of the non-concatenation axis.

        Returns:
            SnowflakeQueryCompiler for concatenated objects.

        Notes:
            If frames have incompatible column/row indices we flatten the
            indices (same as what native pandas does in some cases) to make
            them compatible.
            For example if following two frames being concatenated has following column
            indices:
            column index for frame 1:
            pd.MultiIndex.from_tuples([('a', 'b'), ('c', 'd')], names=['x', 'y'])
            column index for frame 2:
            pd.Index(['e', 'f'])
            Column index of contentated index will be:
            pd.Index([('a', 'b'), ('c', 'd'), 'e', 'f'])
            NOTE: Original column level names are lost and result column index has only
            one level.
        """
        if levels is not None:
            raise NotImplementedError(
                "Snowpark pandas doesn't support 'levels' argument in concat API"
            )
        frames = [self._modin_frame] + [o._modin_frame for o in other]
        for frame in frames:
            self._raise_not_implemented_error_for_timedelta(frame=frame)

        # If index columns differ in size or name, convert all multi-index row labels to
        # tuples with single level index.
        index_columns = self._modin_frame.index_column_snowflake_quoted_identifiers
        index_columns_different = not all(
            f.index_column_snowflake_quoted_identifiers == index_columns for f in frames
        )
        is_mixed_index_multiindex = len({f.num_index_columns for f in frames}) > 1
        is_multiindex = any(f.num_index_columns > 1 for f in frames)

        if is_mixed_index_multiindex or (not is_multiindex and index_columns_different):
            # If ignore_index is True on axis = 0 we fix index compatibility by doing
            # reset and drop all indices.
            if axis == 0 and ignore_index:
                frames = [
                    SnowflakeQueryCompiler(f).reset_index(drop=True)._modin_frame
                    for f in frames
                ]
            else:
                frames = [
                    concat_utils.convert_to_single_level_index(f, axis=0)
                    for f in frames
                ]

        # When concatenating frames where column indices are not compatible, native
        # pandas behavior is not consistent and hard to explain.
        # In native pandas concatenating frame with incompatible column indices will
        # succeed sometimes by flattening the multiindex to make them compatible.
        # (Refer to pandas.Index.to_flat_index to understand index flattening)
        # For Example:
        # >>> df1 = pd.DataFrame([1], columns=["a"])
        # >>> df2 = pd.DataFrame([2], columns=pd.MultiIndex.from_tuples([('a', 'b')]))
        # >>> pd.concat([df1, df2])
        #      a  (a, b)
        # 0	  1.0	NaN
        # 0	  NaN	2.0
        #
        # But sometimes it fails with one of following very unhelpful errors.
        # ValueError: Length of names must match number of levels in MultiIndex.
        # ValueError: no types given
        # IndexError: tuple index out of range
        # ValueError: non-broadcastable output operand with shape ... doesn't match the broadcast shape ...
        # ValueError: operands could not be broadcast together with shapes ...
        #
        # In Snowpark pandas, we provide consistent behavior by always succeeding
        # the concat. If frames have incompatible column indices we flatten the
        # column indices (same as what native pandas does in some cases) to make
        # them compatible.
        if not all(
            join_utils.is_column_index_compatible(frames[0], f) for f in frames[1:]
        ):
            frames = [
                concat_utils.convert_to_single_level_index(f, axis=1) for f in frames
            ]

        # Preserve these index column names whenever possible. If all input
        # objects share a common name, this name will be assigned to the
        # result. When the input names do not all agree, the result will be
        # unnamed. The same is true for MultiIndex, but the logic is applied
        # separately on a level-by-level basis.
        index_column_labels = frames[0].index_column_pandas_labels
        for other_frame in frames[1:]:
            index_column_labels = [
                name1 if name1 == name2 else None
                for name1, name2 in zip(
                    index_column_labels, other_frame.index_column_pandas_labels
                )
            ]

        frames = [
            SnowflakeQueryCompiler(f).set_index_names(index_column_labels)._modin_frame
            for f in frames
        ]
        if axis == 1:
            result_frame = frames[0]
            for other_frame in frames[1:]:
                # Concat on axis = 1 is implemented using align operation. This is
                # equivalent to align on index columns when index labels are same for
                # both the frames.
                # We rename index labels to make sure index columns are aligned level
                # by level.
                if sort is True:
                    align_sort = "sort"
                else:
                    align_sort = "no_sort"
                result_frame, _ = join_utils.align_on_index(
                    result_frame, other_frame, how=join, sort=align_sort
                )

            qc = SnowflakeQueryCompiler(result_frame)

            if ignore_index:
                qc = qc.set_columns(native_pd.RangeIndex(len(qc.columns)))
            elif keys is not None:
                columns = concat_utils.add_keys_as_column_levels(
                    qc.columns, frames, keys, names
                )
                qc = qc.set_columns(columns)
        else:  # axis = 0
            # Add key as outermost index levels.
            if keys and not ignore_index:
                frames = [
                    concat_utils.add_key_as_index_columns(frame, key)
                    for key, frame in zip(keys, frames)
                ]

            # Ensure rows position column and add a new ordering column for global
            # ordering.
            for i, frame in enumerate(frames):
                frames[i] = concat_utils.add_global_ordering_columns(frame, i + 1)

            result_frame = frames[0]
            for other_frame in frames[1:]:
                result_frame = concat_utils.union_all(
                    result_frame, other_frame, join, sort
                )

            qc = SnowflakeQueryCompiler(result_frame)
            if ignore_index:
                qc = qc.reset_index(drop=True)
            elif keys and names:
                # Fill with 'None' to match the number of index columns.
                while len(names) < frames[0].num_index_columns:
                    names.append(None)
                qc = qc.set_index_names(names)

        # If ignore_index is True, it will assign new index values which will not have
        # any duplicates. So there is no need to verify index integrity when
        # ignore_index is True.
        if verify_integrity and not ignore_index:
            if not qc._modin_frame.has_unique_index(axis=axis):
                # Same error as native pandas.
                if axis == 1:
                    overlap = qc.columns[qc.columns.duplicated()].unique()
                    # native pandas raises ValueError: Indexes have overlapping values...
                    # We use different error message for clarity.
                    raise ValueError(f"Columns have overlapping values: {overlap}")
                else:
                    snowflake_ids = (
                        qc._modin_frame.index_column_snowflake_quoted_identifiers
                    )
                    # There can be large number of duplicates, only fetch 10
                    # values to client.
                    limit = 10
                    rows = (
                        qc._modin_frame.ordered_dataframe.group_by(
                            snowflake_ids, count(col("*")).alias("count")
                        )
                        .filter(col("count") > 1)
                        .limit(limit)
                        .select(snowflake_ids)
                        .collect()
                    )
                    overlap = []
                    for row in rows:
                        values = row.as_dict().values()
                        overlap.append(
                            tuple(values) if len(values) > 1 else list(values)[0]
                        )
                    overlap = native_pd.Index(overlap)
                    if len(overlap) < limit:
                        # Same error as native pandas
                        raise ValueError(f"Indexes have overlapping values: {overlap}")
                    else:
                        # In case of large overlaps, Snowpark pandas display different
                        # error message.
                        raise ValueError(
                            f"Indexes have overlapping values. Few of them are: {overlap}. Please run df1.index.intersection(df2.index) to see complete list"
                        )
        # If each input's `attrs` was identical and not empty, then copy it to the output.
        # Otherwise, leave `attrs` empty.
        if len(self._attrs) > 0 and all(self._attrs == o._attrs for o in other):
            qc._attrs = copy.deepcopy(self._attrs)
        return qc

    def cumsum(
        self, axis: int = 0, skipna: bool = True, *args: Any, **kwargs: Any
    ) -> "SnowflakeQueryCompiler":
        """
        Return cumulative sum over a DataFrame or Series axis.

        Args:
            axis : {0 or 1}, default 0
                Axis to compute the cumulative sum along.
            skipna : bool, default True
                Exclude NA/null values. If an entire row/column is NA, the result will be NA.
            *args, **kwargs :
                Additional keywords have no effect but might be accepted for compatibility with NumPy.

        Returns:
            SnowflakeQueryCompiler instance with cumulative sum of Series or DataFrame.
        """
        self._raise_not_implemented_error_for_timedelta()

        if axis == 1:
            ErrorMessage.not_implemented("cumsum with axis=1 is not supported yet")

        cumagg_col_to_expr_map = get_cumagg_col_to_expr_map_axis0(self, sum_, skipna)
        return SnowflakeQueryCompiler(
            self._modin_frame.update_snowflake_quoted_identifiers_with_expressions(
                cumagg_col_to_expr_map
            ).frame
        )

    def cummin(
        self, axis: int = 0, skipna: bool = True, *args: Any, **kwargs: Any
    ) -> "SnowflakeQueryCompiler":
        """
        Return cumulative min over a DataFrame or Series axis.

        Args:
            axis : {0 or 1}, default 0
                Axis to compute the cumulative min along.
            skipna : bool, default True
                Exclude NA/null values. If an entire row/column is NA, the result will be NA.
            *args, **kwargs :
                Additional keywords have no effect but might be accepted for compatibility with NumPy.

        Returns:
            SnowflakeQueryCompiler instance with cumulative min of Series or DataFrame.
        """
        self._raise_not_implemented_error_for_timedelta()

        if axis == 1:
            ErrorMessage.not_implemented("cummin with axis=1 is not supported yet")

        cumagg_col_to_expr_map = get_cumagg_col_to_expr_map_axis0(self, min_, skipna)
        return SnowflakeQueryCompiler(
            self._modin_frame.update_snowflake_quoted_identifiers_with_expressions(
                cumagg_col_to_expr_map
            ).frame
        )

    def cummax(
        self, axis: int = 0, skipna: bool = True, *args: Any, **kwargs: Any
    ) -> "SnowflakeQueryCompiler":
        """
        Return cumulative max over a DataFrame or Series axis.

        Args:
            axis : {0 or 1}, default 0
                Axis to compute the cumulative max along.
            skipna : bool, default True
                Exclude NA/null values. If an entire row/column is NA, the result will be NA.
            *args, **kwargs :
                Additional keywords have no effect but might be accepted for compatibility with NumPy.

        Returns:
            SnowflakeQueryCompiler instance with cumulative max of Series or DataFrame.
        """
        self._raise_not_implemented_error_for_timedelta()

        if axis == 1:
            ErrorMessage.not_implemented("cummax with axis=1 is not supported yet")

        cumagg_col_to_expr_map = get_cumagg_col_to_expr_map_axis0(self, max_, skipna)
        return SnowflakeQueryCompiler(
            self._modin_frame.update_snowflake_quoted_identifiers_with_expressions(
                cumagg_col_to_expr_map
            ).frame
        )

    def melt(
        self,
        id_vars: list[str],
        value_vars: list[str],
        var_name: Optional[str],
        value_name: Optional[str],
        col_level: Optional[int] = None,
        ignore_index: bool = True,
    ) -> "SnowflakeQueryCompiler":
        """
        Unpivot dataframe from wide to long format. The order
        of the data is sorted by column order. Mixed types are
        promoted to Variant.

        Args:
            id_vars : list of identifiers to retain in the result
            value_vars : list of columns to unpivot on
            var_name : variable name, defaults to "variable"
            value_name : value name, defaults to "value"
            col_level : int, not implemented
            ignore_index : bool, ignore the index

        Returns:
            SnowflakeQueryCompiler
                New QueryCompiler with unpivoted data.

        Notes:
            melt does not yet handle multiindex or ignore index
        """
        if col_level is not None:
            raise NotImplementedError(
                "Snowpark Pandas doesn't support 'col_level' argument in melt API"
            )
        if self._modin_frame.is_multiindex(axis=1):
            raise NotImplementedError(
                "Snowpark Pandas doesn't support multiindex columns in melt API"
            )

        frame = self._modin_frame
        value_var_list = frame.data_column_pandas_labels
        for c in id_vars:
            value_var_list.remove(c)
        for c in value_vars:
            value_var_list.append(c)

        if len(frame.data_column_snowflake_quoted_identifiers) <= 0:
            return unpivot_empty_df()

        new_internal_frame = unpivot(
            frame,
            id_vars,
            value_vars,
            var_name,
            value_name,
            ignore_index,
        )
        return SnowflakeQueryCompiler(new_internal_frame)

    def merge(
        self,
        right: "SnowflakeQueryCompiler",
        how: JoinTypeLit,
        on: Optional[IndexLabel] = None,
        left_on: Optional[
            Union[
                Hashable,
                "SnowflakeQueryCompiler",
                list[Union[Hashable, "SnowflakeQueryCompiler"]],
            ]
        ] = None,
        right_on: Optional[
            Union[
                Hashable,
                "SnowflakeQueryCompiler",
                list[Union[Hashable, "SnowflakeQueryCompiler"]],
            ]
        ] = None,
        left_index: Optional[bool] = False,
        right_index: Optional[bool] = False,
        sort: Optional[bool] = False,
        suffixes: Suffixes = ("_x", "_y"),
        copy: Optional[bool] = True,
        indicator: Optional[Union[bool, str]] = False,
        validate: Optional[str] = None,
    ) -> "SnowflakeQueryCompiler":
        """
        Merge with SnowflakeQueryCompiler object to perform Database-style join.

        Args:
            right: other SnowflakeQueryCompiler to merge with.
            how: {'left', 'right', 'outer', 'inner', 'cross'}
                Type of merge to be performed.
            on: Labels or list of such to join on.
            left_on: join keys for left QueryCompiler it can be a label, QueryCompiler
                or a list of such. QueryCompiler join key represents an external data
                that should be used for join as if this is a column from left
                QueryCompiler.
            right_on: join keys for right QueryCompiler it can be a label, QueryCompiler
                or a list of such. QueryCompiler join key represents an external data
                that should be used for join as if this is a column from right
                QueryCompiler.
            left_index: If True, use index from left QueryCompiler as join keys. If it
                is a MultiIndex, the number of keys in the other QueryCompiler (either
                the index or a number of columns) must match the number of levels.
            right_index: If True, use index from right QueryCompiler as join keys. Same
                caveats as 'left_index'.
            sort: If True, sort the result QueryCompiler on join keys lexicographically.
                If False, preserve the order from left QueryCompiler and for ties
                preserve the order from right QueryCompiler.
            suffixes: A length-2 sequence where each element is optionally a string
                indicating the suffix to add to overlapping column names in left and
                right respectively.
            copy: Not used.
            indicator: If True, adds a column to the output DataFrame called "_merge"
                with information on the source of each row. The column can be given a
                different name by providing a string argument. The column will have a
                String type with the value of "left_only" for observations whose merge
                key only appears in the left QueryCompiler, "right_only" for
                observations whose merge key only appears in the right QueryCompiler,
                and "both" if the observation’s merge key is found in both
                QueryCompilers.
            validate: If specified, checks if merge is of specified type.
                "one_to_one" or "1:1": check if merge keys are unique in both left and
                    right datasets.
                "one_to_many" or "1:m": check if merge keys are unique in left dataset.
                "many_to_one" or "m:1": check if merge keys are unique in right dataset.
                "many_to_many" or "m:m": allowed, but does not result in checks.

        Returns:
            SnowflakeQueryCompiler instance with merged result.
        """
        if validate:
            ErrorMessage.not_implemented(
                "Snowpark pandas merge API doesn't yet support 'validate' parameter"
            )

        left = self
        join_index_on_index = left_index and right_index
        # As per this bug fix in pandas 2.2.x outer join always produce sorted results.
        # https://github.com/pandas-dev/pandas/pull/54611/files
        if how == "outer":
            sort = True

        # Labels of indicator columns in input frames.  We use these columns to generate
        # final indicator column in merged frame.
        base_indicator_column_labels = []
        if indicator:
            suffix = generate_random_alphanumeric()
            left_label = f"left_indicator_{suffix}"
            right_label = f"right_indicator_{suffix}"
            # Value is not important here. While generating final indicator columns in
            # merged frame we only check if this is null or not. Any non-null value will
            # work here.
            left = left.insert(0, left_label, 1)
            right = right.insert(0, right_label, 1)
            base_indicator_column_labels = [left_label, right_label]

        if how == "cross" or join_index_on_index:
            # 1. In cross join we join every row from left frame to every row in right
            # frame. This doesn't require any join keys.

            # 2. Joining on index-to-index behavior is very different from joining
            # columns-to-columns or columns-to-index. So we have different code path
            # 'join_on_index_columns' to handle this. Here we create empty keys to
            # share the code of renaming conflicting data column labels.
            left_keys = []
            right_keys = []
            common_join_keys = []
            external_join_keys = []
        else:
            left_keys, right_keys = join_utils.get_join_keys(
                left._modin_frame,
                right._modin_frame,
                on,
                left_on,
                right_on,
                left_index,
                right_index,
            )
            # If a join key is an array-like object frontend converts them to Series and
            # underlying query compiler is passed as join key here.
            # To join on such keys we
            # 1. Insert these as column to original frame.
            # 2. Then join using labels for these inserted columns.
            (
                left,
                left_keys,
                right,
                right_keys,
                external_join_keys,
            ) = join_utils.insert_external_join_keys_into_join_frames(
                left, left_keys, right, right_keys
            )
            # List of join keys where name of left join label is same as right join label.
            # These labels are ignored when we rename labels to resolve conflicts.
            common_join_keys = [
                lkey for lkey, rkey in zip(left_keys, right_keys) if lkey == rkey
            ]

        # Rename conflicting data column pandas labels.
        left_frame, right_frame = join_utils.rename_conflicting_data_column_labels(
            left, right, common_join_keys, suffixes
        )

        if join_index_on_index:
            # Joining on index-to-index behavior is very different from joining
            # columns-to-columns or columns-to-index. So we have different code path to
            # handle this.
            merged_frame, _ = join_utils.join_on_index_columns(
                left_frame, right_frame, how=how, sort=sort
            )
            return SnowflakeQueryCompiler(merged_frame)

        coalesce_config = join_utils.get_coalesce_config(
            left_keys=left_keys,
            right_keys=right_keys,
            external_join_keys=external_join_keys,
        )

        # Update given join keys to labels from renamed frame.
        left_keys = join_utils.map_labels_to_renamed_frame(
            left_keys, left._modin_frame, left_frame
        )
        right_keys = join_utils.map_labels_to_renamed_frame(
            right_keys, right._modin_frame, right_frame
        )

        # Error checking for missing and duplicate labels is already done in frontend
        # layer, so it's safe to use first element from mapped identifiers.
        left_on_identifiers = [
            ids[0]
            for ids in left_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                left_keys
            )
        ]
        right_on_identifiers = [
            ids[0]
            for ids in right_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                right_keys
            )
        ]
        merged_frame = join_utils.join(
            left_frame,
            right_frame,
            how=how,
            left_on=left_on_identifiers,
            right_on=right_on_identifiers,
            sort=sort,
            join_key_coalesce_config=coalesce_config,
        ).result_frame

        # Add indicator column
        if indicator:
            (
                left_ids,
                right_ids,
            ) = merged_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                base_indicator_column_labels
            )
            # Indicator columns have unique labels.
            left_indicator_col = col(left_ids[0])
            right_indicator_col = col(right_ids[0])
            indicator_column_value = (
                when(left_indicator_col.is_null(), "right_only")
                .when(right_indicator_col.is_null(), "left_only")
                .otherwise("both")
            )

            # By default, pandas adds a column called "_merge". The column can be given
            # a different name by providing a string argument.
            indicator_column_label = (
                indicator if isinstance(indicator, str) else "_merge"
            )
            merged_frame = merged_frame.append_column(
                indicator_column_label, indicator_column_value
            )

            # Drop the base indicator columns.
            merged_frame = (
                SnowflakeQueryCompiler(merged_frame)
                .drop(columns=base_indicator_column_labels)
                ._modin_frame
            )

        merged_qc = SnowflakeQueryCompiler(merged_frame)

        # If an index column from left frame is joined with data column from right
        # frame and both have same name, pandas moves this index column to data column.
        index_levels_to_move = []
        for lkey, rkey in zip(left_keys, right_keys):
            if (
                lkey == rkey
                and lkey in left_frame.index_column_pandas_labels
                and rkey in right_frame.data_column_pandas_labels
            ):
                index_levels_to_move.append(
                    left._modin_frame.index_column_pandas_labels.index(lkey)
                )
        if index_levels_to_move:
            merged_qc = merged_qc.reset_index(level=index_levels_to_move)

        if not left_index and not right_index:
            # To match native pandas behavior, reset index if left_index and right_index
            # both are false.
            merged_qc = merged_qc.reset_index(drop=True)

        return merged_qc

    def merge_asof(
        self,
        right: "SnowflakeQueryCompiler",
        on: Optional[str] = None,
        left_on: Optional[str] = None,
        right_on: Optional[str] = None,
        left_index: bool = False,
        right_index: bool = False,
        by: Optional[Union[str, list[str]]] = None,
        left_by: Optional[str] = None,
        right_by: Optional[str] = None,
        suffixes: Suffixes = ("_x", "_y"),
        tolerance: Optional[Union[int, Timedelta]] = None,
        allow_exact_matches: bool = True,
        direction: str = "backward",
    ) -> "SnowflakeQueryCompiler":
        """
        Perform a merge by key distance.

        This is similar to a left-join except that we match on nearest key rather than equal keys.
        Both DataFrames must be sorted by the key. For each row in the left DataFrame:

        A “backward” search selects the last row in the right DataFrame whose ‘on’ key is less than or equal to the left’s key.
        A “forward” search selects the first row in the right DataFrame whose ‘on’ key is greater than or equal to the left’s key.
        A “nearest” search selects the row in the right DataFrame whose ‘on’ key is closest in absolute distance to the left’s key.

        Optionally match on equivalent keys with ‘by’ before searching with ‘on’.

        Parameters
        ----------
        right: other SnowflakeQueryCompiler to merge with.
        on : label
            Field name to join on. Must be found in both DataFrames. The data MUST be ordered.
            Furthermore, this must be a numeric column such as datetimelike, integer, or float.
            On or left_on/right_on must be given.
        left_on : label
            Field name to join on in left DataFrame.
        right_on : label
            Field name to join on in right DataFrame.
        left_index : bool
            Use the index of the left DataFrame as the join key.
        right_index : bool
            Use the index of the right DataFrame as the join key.
        by : column name or list of column names
            Match on these columns before performing merge operation.
        left_by : column name
            Field names to match on in the left DataFrame.
        right_by : column name
            Field names to match on in the right DataFrame.
        suffixes : 2-length sequence (tuple, list, …)
            Suffix to apply to overlapping column names in the left and right side, respectively.
        tolerance: int or Timedelta, optional, default None
            Select asof tolerance within this range; must be compatible with the merge index.
        allow_exact_matches : bool, default True
            If True, allow matching with the same ‘on’ value (i.e. less-than-or-equal-to / greater-than-or-equal-to)
            If False, don’t match the same ‘on’ value (i.e., strictly less-than / strictly greater-than).
        direction : ‘backward’ (default), ‘forward’, or ‘nearest’
            Whether to search for prior, subsequent, or closest matches.

        Returns
        -------
        SnowflakeQueryCompiler
        """
        # TODO: SNOW-1634547: Implement remaining parameters by leveraging `merge` implementation
        if tolerance or suffixes != ("_x", "_y"):
            ErrorMessage.not_implemented(
                "Snowpark pandas merge_asof method does not currently support parameters "
                + "'suffixes', or 'tolerance'"
            )
        if direction not in ("backward", "forward"):
            ErrorMessage.not_implemented(
                "Snowpark pandas merge_asof method only supports directions 'forward' and 'backward'"
            )

        if direction == "backward":
            match_comparator = (
                MatchComparator.GREATER_THAN_OR_EQUAL_TO
                if allow_exact_matches
                else MatchComparator.GREATER_THAN
            )
        else:
            match_comparator = (
                MatchComparator.LESS_THAN_OR_EQUAL_TO
                if allow_exact_matches
                else MatchComparator.LESS_THAN
            )

        left_frame = self._modin_frame
        right_frame = right._modin_frame
        # Get the left and right matching key and quoted identifier corresponding to the match_condition
        # There will only be matching key/identifier for each table as there is only a single match condition
        left_match_keys, right_match_keys = join_utils.get_join_keys(
            left=left_frame,
            right=right_frame,
            on=on,
            left_on=left_on,
            right_on=right_on,
            left_index=left_index,
            right_index=right_index,
        )
        left_match_identifier = (
            left_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                left_match_keys
            )[0][0]
        )
        right_match_identifier = (
            right_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                right_match_keys
            )[0][0]
        )
        coalesce_config = join_utils.get_coalesce_config(
            left_keys=left_match_keys,
            right_keys=right_match_keys,
            external_join_keys=[],
        )

        # Get the left and right matching keys and quoted identifiers corresponding to the 'on' condition
        if by or (left_by and right_by):
            left_on_keys, right_on_keys = join_utils.get_join_keys(
                left=left_frame,
                right=right_frame,
                on=by,
                left_on=left_by,
                right_on=right_by,
            )
            left_on_identifiers = [
                ids[0]
                for ids in left_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                    left_on_keys
                )
            ]
            right_on_identifiers = [
                ids[0]
                for ids in right_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                    right_on_keys
                )
            ]
            coalesce_config.extend(
                join_utils.get_coalesce_config(
                    left_keys=left_on_keys,
                    right_keys=right_on_keys,
                    external_join_keys=[],
                )
            )
        else:
            left_on_identifiers = []
            right_on_identifiers = []

        joined_frame, _ = join_utils.join(
            left=left_frame,
            right=right_frame,
            left_on=left_on_identifiers,
            right_on=right_on_identifiers,
            how="asof",
            left_match_col=left_match_identifier,
            right_match_col=right_match_identifier,
            match_comparator=match_comparator,
            join_key_coalesce_config=coalesce_config,
            sort=True,
        )
        return SnowflakeQueryCompiler(joined_frame)

    def _apply_with_udtf_and_dynamic_pivot_along_axis_1(
        self,
        func: Union[AggFuncType, UserDefinedFunction],
        raw: bool,
        result_type: Optional[Literal["expand", "reduce", "broadcast"]],
        args: tuple,
        column_index: native_pd.Index,
        input_types: list[DataType],
        partition_size: int = DEFAULT_UDTF_PARTITION_SIZE,
        **kwargs: Any,
    ) -> "SnowflakeQueryCompiler":
        """
        Process apply along axis=1 via UDTF and dynamic pivot.

        Args:
            func:
            raw: argument passed to internal df.apply
            result_type: argument passed to internal df.apply
            args: argument passed to internal df.apply
            column_index: index object holding columnar labels of original DataFrame
            input_types: Snowpark types of columns represented by column_index
            partition_size: The batch size in rows the UDTF receives at max. Per default set to DEFAULT_UDTF_PARTITION_SIZE.
            **kwargs:  argument passed to internal df.apply

        Returns:
            SnowflakeQueryCompiler which may be Series or DataFrame representing result of .apply(axis=1)
        """
        self._raise_not_implemented_error_for_timedelta()

        # Process using general approach via UDTF + dynamic pivot to handle column expansion case.

        # Overwrite partition-size with kwargs arg
        if "snowpark_pandas_partition_size" in kwargs:
            partition_size = kwargs["snowpark_pandas_partition_size"]
            kwargs.pop("snowpark_pandas_partition_size")

        # add a row position column for partition by
        # the every batch size in vectorized udtf will be 1
        new_internal_df = self._modin_frame.ensure_row_position_column()
        row_position_snowflake_quoted_identifier = (
            new_internal_df.row_position_snowflake_quoted_identifier
        )

        # The apply function is encapsulated in a UDTF and run as a stored procedure on the pandas dataframe.
        func_udtf = create_udtf_for_apply_axis_1(
            row_position_snowflake_quoted_identifier,
            func,
            raw,
            result_type,
            args,
            column_index,
            input_types,
            self._modin_frame.ordered_dataframe.session,
            **kwargs,
        )

        # Let's start with an example to make the following implementation more clear:
        #
        # We have a Snowpark pandas DataFrame:
        #      A    b
        #      x    y
        # 0  1.1  2.2
        # 1  3.0  NaN
        # with column level names (foo, bar)
        #
        # The underlying Snowpark DataFrame with row position column:
        # ----------------------------------------------------------------------
        # |"__index__"  |"(""A"",""x"")" |"(""b"",""y"")" |"__row_position__"  |
        # ----------------------------------------------------------------------
        # |0            |1.1             |2.2             |0                   |
        # |1            |3.0             |NULL            |1                   |
        # ----------------------------------------------------------------------
        # The function is encapsulated in a UDTF (func_udtf) through helper function called earlier, for this example:
        #    func=lambda x: x+1

        # Apply udtf on data columns and partition by row position column into micro batches of maximum size
        # partition_size.
        # index columns remain unchanged after apply()
        # Calling a (v)UDTF requires a PARTITION BY clause. Here, a vectorized UDF is used (pandas Snowpark types will
        # make the UDTF vectorized).
        partition_identifier = (
            new_internal_df.ordered_dataframe.generate_snowflake_quoted_identifiers(
                pandas_labels=["partition_id"]
            )[0]
        )
        partition_expression = (
            snowpark_round(
                col(row_position_snowflake_quoted_identifier)
                / pandas_lit(partition_size),
                _emit_ast=self._modin_frame.ordered_dataframe.session.ast_enabled,
            )
        ).as_(partition_identifier)
        udtf_dataframe = new_internal_df.ordered_dataframe.select(
            partition_expression,
            row_position_snowflake_quoted_identifier,
            *new_internal_df.data_column_snowflake_quoted_identifiers,
        ).select(
            func_udtf(
                row_position_snowflake_quoted_identifier,
                *new_internal_df.data_column_snowflake_quoted_identifiers,
            ).over(partition_by=[partition_identifier]),
        )

        # NOTE we are keeping the cache_result for performance reasons. DO NOT
        # REMOVE the cache_result unless you can prove that doing so will not
        # materially slow down CI or individual groupby.apply() calls.
        # TODO(SNOW-1345395): Investigate why and to what extent the cache_result
        # is useful.
        ordered_dataframe = cache_result(udtf_dataframe)

        # After applying the udtf, the underlying Snowpark DataFrame becomes
        # -------------------------------------------------------------------------------------------
        # |"__row_position__"  |"LABEL"                                                   |"VALUE"  |
        # -------------------------------------------------------------------------------------------
        # |0                   |{"pos": 0, "0": "A", "1": "x", "names": ["foo", "bar"] }  |2.1      |
        # |0                   |{"pos": 1, "0": "b", "1": "y", "names": ["foo", "bar"] }  |3.2      |
        # |1                   |{"pos": 0, "0": "A", "1": "x", "names": ["foo", "bar"] }  |4        |
        # |1                   |{"pos": 1, "0": "b", "1": "y", "names": ["foo", "bar"] }  |null     |
        # -------------------------------------------------------------------------------------------
        # the row position column is ensured and maintained because we partition by the row position column

        # perform dynamic pivot
        # We pivot on the label column so every label can create a column,
        # which matches the result from df.apply
        ordered_dataframe = ordered_dataframe.pivot(
            APPLY_LABEL_COLUMN_QUOTED_IDENTIFIER,
            None,
            None,
            min_(APPLY_VALUE_COLUMN_QUOTED_IDENTIFIER),
        )

        # After pivot, the underlying Snowpark DataFrame becomes
        # -----------------------------------------------------------------------------------------
        # |"__row_position__"  | "'{""pos"": 0, ""0"": ""A"",     |  "'{""pos"": , ""0"": ""b"",  |
        # |                    |    ""1"": ""x"",  ""names"":     |     ""1"": ""y"", ""names"":  |
        # |                    |    [""foo"", ""bar""] }'"        |     [""foo"", ""bar""] }'     |
        # -----------------------------------------------------------------------------------------
        # |1                   |4                                 |null                           |
        # |0                   |2.1                               |3.2                            |
        # -----------------------------------------------------------------------------------------

        data_column_snowflake_quoted_identifiers = (
            ordered_dataframe.projected_column_snowflake_quoted_identifiers
        )

        assert (
            row_position_snowflake_quoted_identifier
            in data_column_snowflake_quoted_identifiers
        ), "row position identifier must be present after pivot"
        data_column_snowflake_quoted_identifiers.remove(
            row_position_snowflake_quoted_identifier
        )

        # The pivot result can contain multi-level columns, so we need to inspect the column names.  First, we sample
        # a column to determine the number of multi-index levels.  We parse the column name as a k,v dict object.
        object_map = parse_snowflake_object_construct_identifier_to_map(
            data_column_snowflake_quoted_identifiers[0]
        )

        # If there's a "names" key this corresponds to the column index names for each level.  This will only happen
        # if the function maps dataframe -> series, otherwise it must map series -> scalar.
        if "names" in object_map:
            column_index_names = object_map["names"]
            num_column_index_levels = len(column_index_names)

            # Extract the pandas labels and any additional kv map information returned by ApplyFunc.
            (data_column_pandas_labels, data_column_kv_maps,) = list(
                zip(
                    *[
                        parse_object_construct_snowflake_quoted_identifier_and_extract_pandas_label(
                            data_column_snowflake_quoted_identifier,
                            num_column_index_levels,
                        )
                        for data_column_snowflake_quoted_identifier in data_column_snowflake_quoted_identifiers
                    ]
                )
            )

            # If any of the column index names do not match, then pandas uses None values.
            if any(column_index_names != kv["names"] for kv in data_column_kv_maps):
                column_index_names = [None] * num_column_index_levels

            # Look at all the positions, if there's only one position value per label, then we default to the order
            # dictated by those positions.  For example, if output columns by position are [2,3,1] then that's the
            # expected result order.
            data_column_positions = [kv["pos"] for kv in data_column_kv_maps]
            assert len(set(data_column_positions)) == len(data_column_positions)
            (
                data_column_pandas_labels,
                data_column_snowflake_quoted_identifiers,
            ) = sort_apply_udtf_result_columns_by_pandas_positions(
                data_column_positions,
                data_column_pandas_labels,
                data_column_snowflake_quoted_identifiers,
            )

        else:
            # This is the series -> scalar case in which case there are no column labels.
            column_index_names = [None]
            data_column_pandas_labels = [MODIN_UNNAMED_SERIES_LABEL]

        renamed_data_column_snowflake_quoted_identifiers = (
            new_internal_df.ordered_dataframe.generate_snowflake_quoted_identifiers(
                pandas_labels=data_column_pandas_labels,
                excluded=[row_position_snowflake_quoted_identifier],
            )
        )

        # rename columns and cast
        # also sort on the row position column for the join later
        return_variant, return_type = check_return_variant_and_get_return_type(func)
        ordered_dataframe = ordered_dataframe.select(
            row_position_snowflake_quoted_identifier,
            *[
                # casting if return type is specified
                col(old_quoted_identifier).cast(return_type).as_(quoted_identifier)
                if not return_variant
                else col(old_quoted_identifier).as_(quoted_identifier)
                for old_quoted_identifier, quoted_identifier in zip(
                    data_column_snowflake_quoted_identifiers,
                    renamed_data_column_snowflake_quoted_identifiers,
                )
            ],
        ).sort(OrderingColumn(row_position_snowflake_quoted_identifier))

        # After applying pivot and renaming, the underlying Snowpark DataFrame becomes
        # --------------------------------------------------------
        # |"__row_position__"  |"(""A"",""x"")" |"(""b"",""y"")" |
        # --------------------------------------------------------
        # |1                   |4               |null            |
        # |0                   |2.1             |3.2             |
        # --------------------------------------------------------

        # because we don't include index columns in udtf and pivot, we need to
        # join the result from pivot and the original dataframe with index columns
        # on the row position column to add them back. They are unchanged after apply().
        # also sort on the row position column for the join later

        # Joining requires unique quoted identifiers. However, it may happen that the row_position_quoted_identifier and
        # the index_column_snowflake_quoted_identifiers overlap.
        # remove the row position quoted identifier therefore.
        index_columns = new_internal_df.index_column_snowflake_quoted_identifiers
        if row_position_snowflake_quoted_identifier in index_columns:
            index_columns.remove(row_position_snowflake_quoted_identifier)

        # If there are no index_columns, which is the case when the row position column
        # is also the index, then there is no need to restore the index columns.
        # Else, restore them using a join.
        if len(index_columns) != 0:
            index_columns = [row_position_snowflake_quoted_identifier] + index_columns

            original_ordered_dataframe_with_index = (
                new_internal_df.ordered_dataframe.select(
                    *index_columns,
                ).sort(OrderingColumn(row_position_snowflake_quoted_identifier))
            )
            ordered_dataframe = ordered_dataframe.join(
                original_ordered_dataframe_with_index,
                left_on_cols=[row_position_snowflake_quoted_identifier],
                right_on_cols=[row_position_snowflake_quoted_identifier],
                how="inner",
            )

            # After join, the underlying Snowpark DataFrame becomes
            # ----------------------------------------------------------------------
            # |"__row_position__"  |"(""A"",""x"")" |"(""b"",""y"")" |"__index__"  |
            # ----------------------------------------------------------------------
            # |0                   |2.1             |3.2             |0            |
            # |1                   |4               |null            |0            |
            # ----------------------------------------------------------------------
            # which is the final result and what we want

        new_internal_frame = InternalFrame.create(
            ordered_dataframe=ordered_dataframe,
            data_column_pandas_labels=data_column_pandas_labels,
            data_column_pandas_index_names=column_index_names,
            data_column_snowflake_quoted_identifiers=renamed_data_column_snowflake_quoted_identifiers,
            index_column_pandas_labels=new_internal_df.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=new_internal_df.index_column_snowflake_quoted_identifiers,
            data_column_types=None,
            index_column_types=None,
        )
        return SnowflakeQueryCompiler(new_internal_frame)

    def _apply_udf_row_wise_and_reduce_to_series_along_axis_1(
        self,
        func: Union[AggFuncType, UserDefinedFunction],
        column_index: pandas.Index,
        input_types: list[DataType],
        return_type: DataType,
        session: Session,
        udf_args: tuple = (),
        udf_kwargs: dict = {},  # noqa: B006
    ) -> "SnowflakeQueryCompiler":
        """
        Calls pandas apply API per row yielding a Series. `func` is a function that expects a single input parameter which is passed
        each row as a Series object. E.g., for the following DataFrame

        | index |  A  |  B  |
        |-------|-----|-----|
        | 'idx' |  3  |   2 |

        when calling df.apply(lambda x: x['A'], axis=1) the parameter x
        will be passed as Series object indexed by the original DataFrame's column labels
        and named after the value of the index per row.

        pd.Series([3, 2], index=['A', 'B'], name='idx')

        In the case of a multi-index, the name will be a tuple of the index columns.

        Args:
            func: pandas compatible function or object
            column_index: column index of the original Dataframe
            input_types: Snowpark types of the data columns
            return_type: Snowpark type that func produces.
            udf_args: Positional arguments passed to func after Series value.
            udf_kwargs: Additional keyword arguments passed to fund after Series value and positional arguments.

        Returns:
            SnowflakeQueryCompiler representing a Series holding the result of apply(func, axis=1).
        """
        self._raise_not_implemented_error_for_timedelta()

        # extract index columns and types, which are passed as first columns to UDF.
        index_identifiers = self._modin_frame.index_column_snowflake_quoted_identifiers
        index_types = self._modin_frame.get_snowflake_type(index_identifiers)
        n_index_columns = len(index_types)

        # If func is passed as Snowpark UserDefinedFunction, extract underlying wrapped function and add its packages.
        packages = list(session.get_packages().values())
        if isinstance(func, UserDefinedFunction):
            packages += func._packages
            func = func.func

        # Need to cast columns in wrapper to correct pandas types.
        pandas_column_types = self.dtypes
        pandas_type_map = dict(zip(list(column_index), pandas_column_types))

        # TODO: SNOW-1057497 handling of 3rd party packages required by UDF.
        # create vectorized wrapper restoring column index for row-wise applied UDF func.
        # no coverage here because server-side invocation
        @pandas_udf(
            packages=packages
            + [pandas],  # use here actual pandas module to match version.
            input_types=[PandasDataFrameType(index_types + input_types)],
            return_type=PandasSeriesType(return_type),
            session=session,
        )  # pragma: no cover
        def vectorized_udf(df: pandas.DataFrame) -> pandas.Series:  # pragma: no cover

            # First, set index using the first n_index_columns columns.
            # The name of the columns does not matter here, as they won't be referenced ever again in the handler.
            df.set_index(
                list(df.columns)[:n_index_columns], inplace=True
            )  # pragma: no cover

            # Second, restore column names.
            df.columns = column_index  # pragma: no cover

            # Restore types.
            df = df.astype(pandas_type_map)

            # call apply with result_type='reduce' to force return schema to be a single column.
            # This will also ensure that the result is always a Series object.
            series = df.apply(  # pragma: no cover
                func,
                axis=1,
                result_type="reduce",
                args=udf_args,
                **udf_kwargs,  # pragma: no cover
            )  # pragma: no cover

            return series  # pragma: no cover

        # Apply vUDF per row and append result as new column.
        new_identifier = (
            self._modin_frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
                pandas_labels=["apply_result"]
            )[0]
        )
        new_ordered_frame = append_columns(
            self._modin_frame.ordered_dataframe,
            new_identifier,
            vectorized_udf(
                index_identifiers
                + self._modin_frame.data_column_snowflake_quoted_identifiers
            ),
        )

        # Construct new internal frame based on index columns + the newly returned series column (which is unnamed).
        # The result is always a Series.
        new_frame = InternalFrame.create(
            ordered_dataframe=new_ordered_frame,
            data_column_pandas_labels=[MODIN_UNNAMED_SERIES_LABEL],
            data_column_pandas_index_names=self._modin_frame.data_column_index_names,
            data_column_snowflake_quoted_identifiers=[new_identifier],
            index_column_pandas_labels=self._modin_frame.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=self._modin_frame.index_column_snowflake_quoted_identifiers,
            data_column_types=None,
            index_column_types=None,
        )
        return SnowflakeQueryCompiler(new_frame)

    def align(
        self,
        other: SnowparkDataFrame = None,
        join: str = "outer",
        axis: int = 0,
        level: Level = None,
        copy: bool = True,
        fill_value: Scalar = None,
    ) -> tuple["SnowflakeQueryCompiler", "SnowflakeQueryCompiler"]:
        """
        Align two objects on their axes with the specified join method.

        Join method is specified for each axis Index.

        Args:
            other: DataFrame or Series
            join: {‘outer’, ‘inner’, ‘left’, ‘right’}, default ‘outer’
                Type of alignment to be performed.
                left: use only keys from left frame, preserve key order.
                right: use only keys from right frame, preserve key order.
                outer: use union of keys from both frames, sort keys lexicographically.
                inner: use intersection of keys from both frames, preserve the order of the left keys.
            axis: allowed axis of the other object, default None
                Align on index (0), columns (1), or both (None).
            level: int or level name, default None
                Broadcast across a level, matching Index values on the passed MultiIndex level.
            copy: bool, default True
                Always returns new objects. If copy=False and no reindexing is required then original objects are returned.
            fill_value: scalar, default np.nan
                Always returns new objects. If copy=False and no reindexing is required then original objects are returned.

        Returns:
            tuple of SnowflakeQueryCompilers
            Aligned objects.

        """
        if copy is not True:
            ErrorMessage.not_implemented(
                "Snowpark pandas 'align' method doesn't support 'copy=False'"
            )
        if level is not None:
            ErrorMessage.not_implemented(
                "Snowpark pandas 'align' method doesn't support 'level'"
            )
        if fill_value is not None:
            # TODO: SNOW-1752860
            ErrorMessage.not_implemented(
                "Snowpark pandas 'align' method doesn't support 'fill_value'"
            )

        frame = self._modin_frame
        other_frame = other._query_compiler._modin_frame

        if self.is_multiindex() or other._query_compiler.is_multiindex():
            raise NotImplementedError(
                "Snowpark pandas doesn't support `align` with MultiIndex"
            )

        # convert frames to variant type if index is incompatible for join
        frame, other_frame = join_utils.convert_incompatible_types_to_variant(
            frame,
            other_frame,
            frame.index_column_snowflake_quoted_identifiers,
            other_frame.index_column_snowflake_quoted_identifiers,
        )
        if axis == 0:
            left_internal_frame = align_axis_0_left(frame, other_frame, join)
            right_internal_frame = align_axis_0_right(frame, other_frame, join)

            left_qc = SnowflakeQueryCompiler(left_internal_frame)
            right_qc = SnowflakeQueryCompiler(right_internal_frame)

        if axis == 1:
            left_frame, right_frame = align_axis_1(frame, other_frame, join)
            left_qc, right_qc = SnowflakeQueryCompiler(
                left_frame
            ), SnowflakeQueryCompiler(right_frame)

        if axis is None:
            left_frame_1, right_frame_1 = align_axis_1(frame, other_frame, join)
            left_internal_frame = align_axis_0_left(left_frame_1, right_frame_1, join)
            right_internal_frame = align_axis_0_right(left_frame_1, right_frame_1, join)
            left_qc = SnowflakeQueryCompiler(left_internal_frame)
            right_qc = SnowflakeQueryCompiler(right_internal_frame)

        return left_qc, right_qc

    def apply(
        self,
        func: Union[AggFuncType, UserDefinedFunction],
        axis: int = 0,
        raw: bool = False,
        result_type: Optional[Literal["expand", "reduce", "broadcast"]] = None,
        args: tuple = (),
        **kwargs: Any,
    ) -> "SnowflakeQueryCompiler":
        """
        Apply passed function across given axis.

        Parameters
        ----------
        func : callable(pandas.Series) -> scalar, str, list or dict of such
            The function to apply to each column or row.
        axis : {0, 1}
            Target axis to apply the function along.
            0 is for index, 1 is for columns.
        raw : bool, default: False
            Whether to pass a high-level Series object (False) or a raw representation
            of the data (True).
        result_type : {"expand", "reduce", "broadcast", None}, default: None
            Determines how to treat list-like return type of the `func` (works only if
            a single function was passed):

            - "expand": expand list-like result into columns.
            - "reduce": keep result into a single cell (opposite of "expand").
            - "broadcast": broadcast result to original data shape (overwrite the existing column/row with the function result).
            - None: use "expand" strategy if Series is returned, "reduce" otherwise.
        args : Tuple
            Positional arguments to pass to `func`.
        **kwargs : dict
            Keyword arguments to pass to `func`.
        """
        self._raise_not_implemented_error_for_timedelta()

        # Only callables are supported for axis=1 mode for now.
        if not callable(func) and not isinstance(func, UserDefinedFunction):
            ErrorMessage.not_implemented(
                "Snowpark pandas apply API only supports callables func"
            )
        if result_type is not None:
            ErrorMessage.not_implemented(
                "Snowpark pandas apply API doesn't yet support 'result_type' parameter"
            )
        if check_snowpark_pandas_object_in_arg(
            args
        ) or check_snowpark_pandas_object_in_arg(kwargs):
            ErrorMessage.not_implemented(
                "Snowpark pandas apply API doesn't yet support DataFrame or Series in 'args' or 'kwargs' of 'func'"
            )
        if (
            is_supported_snowpark_python_function(func)
            or func in SUPPORTED_SNOWFLAKE_CORTEX_FUNCTIONS_IN_APPLY
        ):
            if axis != 0:
                ErrorMessage.not_implemented(
                    f"Snowpark pandas apply API doesn't yet support Snowflake Cortex function `{func.__name__}` with with axis = {axis}.'"
                )
            if raw is not False:
                ErrorMessage.not_implemented(
                    f"Snowpark pandas apply API doesn't yet support Snowflake Cortex function `{func.__name__}`with raw = {raw}."
                )
            if args:
                ErrorMessage.not_implemented(
                    f"Snowpark pandas apply API doesn't yet support Snowflake Cortex function `{func.__name__}` with args == '{args}'"
                )
            return self._apply_snowflake_function_to_columns(func, kwargs)
        elif func in ALL_SNOWFLAKE_CORTEX_FUNCTIONS:
            ErrorMessage.not_implemented(
                f"Snowpark pandas apply API doesn't yet support Snowflake Cortex function `{func.__name__}`"
            )

        sf_func = NUMPY_UNIVERSAL_FUNCTION_TO_SNOWFLAKE_FUNCTION.get(func)
        if sf_func is not None:
            return self._apply_snowflake_function_to_columns(sf_func, kwargs)

        if get_snowflake_agg_func(func, {}, axis) is not None:
            # np.std and np.var 'ddof' parameter defaults to 0 but
            # df.std and df.var 'ddof' parameter defaults to 1.
            # Set it here explicitly to 0 if not provided.
            if func in (np.std, np.var) and "ddof" not in kwargs:
                kwargs["ddof"] = 0
            # np.median return NaN if any value is NaN while df.median skips NaN values.
            # Set 'skipna' to false to match behavior.
            if func == np.median:
                kwargs["skipna"] = False
            qc = self.agg(func, axis, None, kwargs)
            if axis == 1:
                # agg method populates series name with aggregation function name but
                # in apply we need unnamed series.
                qc = qc.set_columns([MODIN_UNNAMED_SERIES_LABEL])
            return qc

        if axis == 0:
            frame = self._modin_frame

            # To apply function to Dataframe with axis=0, we repurpose the groupby apply function by taking each
            # column, as a series, and treat as a single group to apply function.  Then collect the column results to
            # join together for the final result.
            col_results = []

            # If raw, then pass numpy ndarray rather than pandas Series as input to the apply function.
            if raw:

                def wrapped_func(*args, **kwargs):  # type: ignore[no-untyped-def] # pragma: no cover: skip coverage for this function because coverage tools can't tell that we're executing this function because we execute it in a UDTF.
                    raw_input_obj = args[0].to_numpy()
                    args = (raw_input_obj,) + args[1:]
                    return func(*args, **kwargs)

                agg_func = wrapped_func
            else:
                agg_func = func

            # Accumulate indices of the column results.
            col_result_indexes = []
            # Accumulate "is scalar" flags for the column results.
            col_result_scalars = []

            # Loop through each data column of the original df frame
            for (column_index, data_column_pair) in enumerate(
                zip(
                    frame.data_column_pandas_labels,
                    frame.data_column_snowflake_quoted_identifiers,
                )
            ):
                (
                    data_column_pandas_label,
                    data_column_snowflake_quoted_identifier,
                ) = data_column_pair

                # Create a frame for the current data column which we will be passed to the apply function below.
                # Note that we maintain the original index because the apply function may access via the index.
                data_col_qc = self.take_2d_positional(
                    index=slice(None, None), columns=[column_index]
                )

                data_col_frame = data_col_qc._modin_frame

                data_col_qc = data_col_qc.groupby_apply(
                    by=[],
                    agg_func=agg_func,
                    axis=0,
                    groupby_kwargs={"as_index": False, "dropna": False},
                    agg_args=args,
                    agg_kwargs=kwargs,
                    series_groupby=True,
                    force_single_group=True,
                    force_list_like_to_series=True,
                    include_groups=True,
                )

                data_col_result_frame = data_col_qc._modin_frame

                # Set the index names and corresponding data column pandas label on the result.
                data_col_result_frame = InternalFrame.create(
                    ordered_dataframe=data_col_result_frame.ordered_dataframe,
                    data_column_snowflake_quoted_identifiers=data_col_result_frame.data_column_snowflake_quoted_identifiers,
                    data_column_pandas_labels=[data_column_pandas_label],
                    data_column_pandas_index_names=data_col_frame.data_column_pandas_index_names,
                    data_column_types=None,
                    index_column_snowflake_quoted_identifiers=data_col_result_frame.index_column_snowflake_quoted_identifiers,
                    index_column_pandas_labels=data_col_result_frame.index_column_pandas_labels,
                    index_column_types=data_col_result_frame.cached_index_column_snowpark_pandas_types,
                )

                data_col_result_index = (
                    data_col_result_frame.index_columns_pandas_index()
                )
                col_result_indexes.append(data_col_result_index)
                # TODO: For functions like np.sum, when supported, we can know upfront the result is a scalar
                # so don't need to look at the index.
                col_result_scalars.append(
                    len(data_col_result_index) == 1 and data_col_result_index[0] == -1
                )
                col_results.append(SnowflakeQueryCompiler(data_col_result_frame))

            result_is_series = False

            if len(col_results) == 1:
                result_is_series = col_result_scalars[0]
                qc_result = col_results[0]

                # Squeeze to series if it is single column
                qc_result = qc_result.columnarize()
                if col_result_scalars[0]:
                    qc_result = qc_result.reset_index(drop=True)
            else:
                single_row_output = all(len(index) == 1 for index in col_result_indexes)
                if single_row_output:
                    all_scalar_output = all(
                        is_scalar for is_scalar in col_result_scalars
                    )
                    if all_scalar_output:
                        # If the apply function maps all columns to a scalar value, then we need to join them together
                        # to return as a Series result.

                        # Ensure all column results have the same column name so concat will be aligned.
                        for i, qc in enumerate(col_results):
                            col_results[i] = qc.set_columns([0])

                        qc_result = col_results[0].concat(
                            axis=0,
                            other=col_results[1:],
                            keys=frame.data_column_pandas_labels,
                        )
                        qc_frame = qc_result._modin_frame

                        # Drop the extraneous index column from the original result series.
                        qc_result = SnowflakeQueryCompiler(
                            InternalFrame.create(
                                ordered_dataframe=qc_frame.ordered_dataframe,
                                data_column_snowflake_quoted_identifiers=qc_frame.data_column_snowflake_quoted_identifiers,
                                data_column_pandas_labels=qc_frame.data_column_pandas_labels,
                                data_column_pandas_index_names=qc_frame.data_column_pandas_index_names,
                                data_column_types=qc_frame.cached_data_column_snowpark_pandas_types,
                                index_column_snowflake_quoted_identifiers=qc_frame.index_column_snowflake_quoted_identifiers[
                                    :-1
                                ],
                                index_column_pandas_labels=qc_frame.index_column_pandas_labels[
                                    :-1
                                ],
                                index_column_types=qc_frame.cached_index_column_snowpark_pandas_types[
                                    :-1
                                ],
                            )
                        )

                        result_is_series = True
                    else:
                        no_scalar_output = all(
                            not is_scalar for is_scalar in col_result_scalars
                        )
                        if no_scalar_output:
                            # Output is Dataframe
                            all_same_index = col_result_indexes.count(
                                col_result_indexes[0]
                            ) == len(col_result_indexes)
                            qc_result = col_results[0].concat(
                                axis=1, other=col_results[1:], sort=not all_same_index
                            )
                        else:
                            # If there's a mix of scalar and pd.Series output from the apply func, pandas stores the
                            # pd.Series output as the value, which we do not currently support.
                            ErrorMessage.not_implemented(
                                "Nested pd.Series in result is not supported in DataFrame.apply(axis=0)"
                            )
                else:
                    if any(is_scalar for is_scalar in col_result_scalars):
                        # If there's a mix of scalar and pd.Series output from the apply func, pandas stores the
                        # pd.Series output as the value, which we do not currently support.
                        ErrorMessage.not_implemented(
                            "Nested pd.Series in result is not supported in DataFrame.apply(axis=0)"
                        )

                    duplicate_index_values = not all(
                        len(i) == len(set(i)) for i in col_result_indexes
                    )

                    # If there are duplicate index values then align on the index for matching results with Pandas.
                    if duplicate_index_values:
                        curr_frame = col_results[0]._modin_frame
                        for next_qc in col_results[1:]:
                            curr_frame = join_utils.align(
                                curr_frame, next_qc._modin_frame, [], [], how="left"
                            ).result_frame
                        qc_result = SnowflakeQueryCompiler(curr_frame)
                    else:
                        # If there are multiple output series with different indices, then line them up as a series output.
                        all_same_index = all(
                            all(i == col_result_indexes[0]) for i in col_result_indexes
                        )
                        # If the col results all have same index then we keep the existing index ordering.
                        qc_result = col_results[0].concat(
                            axis=1, other=col_results[1:], sort=not all_same_index
                        )

            # If result should be Series then change the data column label appropriately.
            if result_is_series:
                qc_result_frame = qc_result._modin_frame
                qc_result = SnowflakeQueryCompiler(
                    InternalFrame.create(
                        ordered_dataframe=qc_result_frame.ordered_dataframe,
                        data_column_snowflake_quoted_identifiers=qc_result_frame.data_column_snowflake_quoted_identifiers,
                        data_column_pandas_labels=[MODIN_UNNAMED_SERIES_LABEL],
                        data_column_pandas_index_names=qc_result_frame.data_column_pandas_index_names,
                        data_column_types=qc_result_frame.cached_data_column_snowpark_pandas_types,
                        index_column_snowflake_quoted_identifiers=qc_result_frame.index_column_snowflake_quoted_identifiers,
                        index_column_pandas_labels=qc_result_frame.index_column_pandas_labels,
                        index_column_types=qc_result_frame.cached_index_column_snowpark_pandas_types,
                    )
                )

            return qc_result
        else:
            # get input types of all data columns from the dataframe directly
            input_types = self._modin_frame.get_snowflake_type(
                self._modin_frame.data_column_snowflake_quoted_identifiers
            )

            from snowflake.snowpark.modin.plugin.extensions.utils import (
                try_convert_index_to_native,
            )

            # current columns
            column_index = try_convert_index_to_native(
                self._modin_frame.data_columns_index
            )

            # Extract return type from annotations (or lookup for known pandas functions) for func object,
            # if no return type could be extracted the variable will hold None.
            return_type = deduce_return_type_from_function(func, None)

            # Check whether return_type has been extracted. If return type is not
            # a Series, tuple or list object, compute df.apply using a vUDF. In this case no column expansion needs to
            # be performed which means that the result of df.apply(axis=1) is always a Series object.
            if return_type and not (
                isinstance(return_type, PandasSeriesType)
                or isinstance(return_type, ArrayType)
            ):
                return self._apply_udf_row_wise_and_reduce_to_series_along_axis_1(
                    func,
                    column_index,
                    input_types,
                    return_type,
                    udf_args=args,
                    udf_kwargs=kwargs,
                    session=self._modin_frame.ordered_dataframe.session,
                )
            else:
                # Issue actionable warning for users to consider annotating UDF with type annotations
                # for better performance.
                function_name = (
                    func.__name__ if isinstance(func, Callable) else str(func)  # type: ignore[arg-type]
                )
                WarningMessage.single_warning(
                    f"Function {function_name} passed to apply does not have type annotations,"
                    f" or Snowpark pandas could not extract type annotations. Executing apply"
                    f" in slow code path which may result in decreased performance. "
                    f"To disable this warning and improve performance, consider annotating"
                    f" {function_name} with type annotations."
                )

                # Result may need to get expanded into multiple columns, or return type of func is not known.
                # Process using UDTF together with dynamic pivot for either case.
                return self._apply_with_udtf_and_dynamic_pivot_along_axis_1(
                    func, raw, result_type, args, column_index, input_types, **kwargs
                )

    def _apply_snowflake_function_to_columns(
        self,
        snowflake_function: Callable,
        kwargs: dict[str, Any],
    ) -> "SnowflakeQueryCompiler":
        """Apply Snowflake function to columns."""

        def sf_function(col: SnowparkColumn) -> SnowparkColumn:
            if not kwargs:
                return snowflake_function(col)
            # we have named kwargs, which may be positional
            # in nature, and we need to align them to the Snowflake
            # function call alongside the column reference

            params = inspect.signature(snowflake_function).parameters
            resolved_positional = []
            found_snowpark_column = False
            kwargs_list = kwargs
            for arg in params:
                if arg in kwargs:
                    resolved_positional.append(kwargs[arg])
                    kwargs_list.pop(arg)
                else:
                    if not found_snowpark_column:
                        resolved_positional.append(col)
                        found_snowpark_column = True
                    # TODO: SNOW-1927811 Kwargs "_emit_ast" and "_ast" appear in the function signature
                    # and will be passed to the function by Snowpark Python so they should not be added as
                    # positional args here
                    elif arg in ("_emit_ast", "_ast"):
                        continue
                    elif (
                        params[arg].default is not inspect.Parameter.empty
                    ):  # pragma: no cover
                        #  If the unspecified arg has a default value, that default value is added
                        #  to the positional arguments.
                        resolved_positional.append(params[arg].default)

                    else:
                        ErrorMessage.not_implemented(
                            f"Unspecified Argument: {arg} - when using apply with kwargs, all function arguments should be specified except the single column reference (if applicable)."
                        )
            if kwargs_list:
                ErrorMessage.not_implemented(
                    f"Unspecified kwargs: {kwargs_list} are not part of function arguments."
                )
            return snowflake_function(*resolved_positional)

        return SnowflakeQueryCompiler(
            self._modin_frame.apply_snowpark_function_to_columns(sf_function)
        )

    def applymap(
        self,
        func: AggFuncType,
        na_action: Optional[Literal["ignore"]] = None,
        args: tuple[Any, ...] = (),
        **kwargs: Any,
    ) -> "SnowflakeQueryCompiler":
        """
        Apply passed function elementwise.

        Parameters
        ----------
        func : callable(scalar) -> scalar
            Function to apply to each element of the QueryCompiler.
        na_action: If 'ignore', propagate NULL values
        *args : iterable
            Positional arguments passed to func after the input data.
        **kwargs : dict
            Additional keyword arguments to pass as keywords arguments to func.
        """
        self._raise_not_implemented_error_for_timedelta()

        if (
            is_supported_snowpark_python_function(func)
            or func in SUPPORTED_SNOWFLAKE_CORTEX_FUNCTIONS_IN_APPLY
        ):
            if na_action:
                ErrorMessage.not_implemented(
                    f"Snowpark pandas applymap API doesn't yet support Snowflake Cortex function `{func.__name__}` with na_action == '{na_action}'"
                )
            if args:
                ErrorMessage.not_implemented(
                    f"Snowpark pandas applymap API doesn't yet support Snowflake Cortex function `{func.__name__}` with args == '{args}'"
                )
            return self._apply_snowflake_function_to_columns(func, kwargs)
        elif func in ALL_SNOWFLAKE_CORTEX_FUNCTIONS:
            ErrorMessage.not_implemented(
                f"Snowpark pandas apply API doesn't yet support Snowflake Cortex function `{func.__name__}`"
            )

        # Check if the function is a known numpy function that can be translated
        # to Snowflake function.
        sf_func = NUMPY_UNIVERSAL_FUNCTION_TO_SNOWFLAKE_FUNCTION.get(func)
        if sf_func is not None:
            return self._apply_snowflake_function_to_columns(sf_func, kwargs)

        if func in (np.sum, np.min, np.max):
            # Aggregate functions applied element-wise to columns are no-op.
            return self

        # Currently, NULL values are always passed into the udtf even if strict=True,
        # which is a bug on the server side SNOW-880105.
        # The fix will not land soon, so we are going to raise not implemented error for now.
        # TODO SNOW-1332314: linked jira is fixed now. Verify and enable this.
        if na_action == "ignore":
            ErrorMessage.not_implemented(
                "Snowpark pandas applymap API doesn't yet support na_action == 'ignore'"
            )

        # create and apply udfs on all data columns
        replace_mapping = {}
        for f in self._modin_frame.ordered_dataframe.schema.fields:
            identifier = f.column_identifier.quoted_name
            if identifier in self._modin_frame.data_column_snowflake_quoted_identifiers:
                return_type = deduce_return_type_from_function(
                    func, f.datatype, **kwargs
                )
                if not return_type:
                    return_type = VariantType()
                func_udf = create_udf_for_series_apply(
                    func,
                    return_type,
                    f.datatype,
                    na_action,
                    self._modin_frame.ordered_dataframe.session,
                    args,
                    **kwargs,
                )
                replace_mapping[identifier] = func_udf(identifier)

        return SnowflakeQueryCompiler(
            self._modin_frame.update_snowflake_quoted_identifiers_with_expressions(
                replace_mapping
            ).frame
        )

    def _map_series_with_dict_like(
        self, mapping: Union[Mapping, native_pd.Series]
    ) -> "SnowflakeQueryCompiler":
        """
        Map existing values to new values according to a dict-like mapping.

        Parameters
        ----------
        mapping : Mapping or native_pd.Series
            Mapping from current values to new values.

        Returns
        -------
        SnowflakeQueryCompiler
        """
        if isinstance(mapping, defaultdict) and mapping.default_factory is None:
            # defaultdict with default_factory = None raises a KeyError if the
            # series includes a key that's not in the defaultdict. We would have
            # to check every element of the series to determine whether to raise
            # a KeyError, so we fall back to the `applymap` implementation
            # using a UD(T)F.
            return self.applymap(lambda v: mapping[v])

        # We implement the mapping as a single CASE/WHEN/ELSE expression. We
        # could implement this method with Series.case_when() or a series of
        # Series.mask() calls, but while both of those implementations also
        # avoid self-joins, they produce much larger queries than this
        # implementation does. case_when() projects out at least one extra
        # variable for each value in the dictionary, while each mask() call adds
        # one level to the query depth.

        assert len(self.columns) == 1, "Internal error: Only Series has a map() method."
        return SnowflakeQueryCompiler(
            self._modin_frame.apply_snowpark_function_to_columns(
                snowpark_func=make_series_map_snowpark_function(
                    mapping=mapping,
                    self_type=self._modin_frame.get_snowflake_type(
                        self._modin_frame.data_column_snowflake_quoted_identifiers[0]
                    ),
                )
            )
        )

    def map(
        self,
        arg: Union[AggFuncType, "pd.Series"],
        na_action: Optional[Literal["ignore"]] = None,
        **kwargs: Any,
    ) -> "SnowflakeQueryCompiler":
        """This method will only be called from Series."""
        self._raise_not_implemented_error_for_timedelta()

        # TODO SNOW-801847: support series.map when arg is a dict/series
        # Currently, NULL values are always passed into the udtf even if strict=True,
        # which is a bug on the server side SNOW-880105.
        # The fix will not land soon, so we are going to raise not implemented error for now.
        # TODO SNOW-1332314: linked jira is fixed now. Verify and enable this
        # both when `arg` is a callable and when it's dict-like.
        if na_action == "ignore":
            ErrorMessage.not_implemented(
                "Snowpark pandas map API doesn't yet support na_action == 'ignore'"
            )
        if callable(arg):
            return self.applymap(func=arg, na_action=na_action, **kwargs)

        if not isinstance(arg, (Mapping, native_pd.Series)):
            raise TypeError(
                "`arg` should be a callable, a Mapping, or a pandas Series, "
                + f"but instead it is of type {type(arg).__name__}"
            )

        return self._map_series_with_dict_like(arg)

    def apply_on_series(
        self, func: AggFuncType, args: tuple[Any, ...] = (), **kwargs: Any
    ) -> "SnowflakeQueryCompiler":
        """
        Apply passed function on underlying Series.

        Parameters
        ----------
        func : callable(pandas.Series) -> scalar, str, list or dict of such
            The function to apply to each row.
        *args : iterable
            Positional arguments to pass to `func`.
        **kwargs : dict
            Keyword arguments to pass to `func`.
        """
        self._raise_not_implemented_error_for_timedelta()

        assert self.is_series_like()

        # TODO SNOW-856682: support other types (str, list, dict) of func
        if not callable(func):
            ErrorMessage.not_implemented(
                "Snowpark pandas apply API only supports callables func"
            )
        if check_snowpark_pandas_object_in_arg(
            args
        ) or check_snowpark_pandas_object_in_arg(kwargs):
            ErrorMessage.not_implemented(
                "Snowpark pandas apply API doesn't yet support DataFrame or Series in 'args' or 'kwargs' of 'func'"
            )

        return self.applymap(func, args=args, **kwargs)

    def is_series_like(self) -> bool:
        """
        Check whether this QueryCompiler can represent ``modin.pandas.Series`` object.

        Returns
        -------
        bool
            Return True if QueryCompiler has a single column, False otherwise.
        """
        return self.get_axis_len(axis=1) == 1

    def pivot(
        self,
        columns: Any,
        index: Optional[Any] = None,
        values: Optional[Any] = None,
    ) -> "SnowflakeQueryCompiler":
        """
        Return reshaped DataFrame organized by given index / column values.

        Reshape data (produce a “pivot” table) based on column values. Uses unique values from
        specified index / columns to form axes of the resulting DataFrame. This function does not
        support data aggregation, multiple values will result in a MultiIndex in the columns.

        Parameters
        ----------
        columns : str or object or a list of str
            Column to use to make new frame’s columns.
        index : str or object or a list of str, optional
            Column to use to make new frame’s index. If not given, uses existing index.
        values : str, object or a list of the previous, optional
            Column(s) to use for populating new frame’s values. If not specified, all remaining columns
            will be used and the result will have hierarchically indexed columns.

        Returns
        -------
        SnowflakeQueryCompiler
        """
        self._raise_not_implemented_error_for_timedelta()

        # Call pivot_table which is a more generalized version of pivot with `min` aggregation
        # Note we differ from pandas by not checking for duplicates and raising a ValueError as that would require an eager query
        return self.pivot_table(
            columns=columns,
            index=index,
            values=values,
            aggfunc="min",
            fill_value=None,
            margins=False,
            dropna=True,
            margins_name="All",
            observed=False,
            sort=True,
        )

    def pivot_table(
        self,
        index: Any,
        values: Any,
        columns: Any,
        aggfunc: AggFuncType,
        fill_value: Optional[Scalar],
        margins: bool,
        dropna: bool,
        margins_name: str,
        observed: bool,
        sort: bool,
    ) -> "SnowflakeQueryCompiler":
        self._raise_not_implemented_error_for_timedelta()

        """
        Create a spreadsheet-style pivot table from underlying data.

        Parameters
        ----------
        index : column or list of the previous, optional
            If an array is passed, it must be the same length as the data.
            The list can contain any of the other types (except list).
            Keys to group by on the pivot table index. If an array is
            passed, it is being used as the same manner as column values.
        values : column to aggregate, or list of the previous, optional
        columns : column or list of previous, optional
            If an array is passed, it must be the same length as the data.
            The list can contain any of the other types (except list).
            Keys to group by on the pivot table column. If an array is
            passed, it is being used as the same manner as column values.
        aggfunc : function, list of functions, dict, default numpy.mean
            If list of functions passed, the resulting pivot table will
            have hierarchical columns whose top level are the function
            names (inferred from the function objects themselves)
            If dict is passed, the key is column to aggregate and value
            is function or list of functions.
        fill_value : scalar, optional
            Value to replace missing values with (in the resulting pivot
            table, after aggregation).
        margins : bool, default False
            Add all row / columns (e.g. for subtotal / grand totals).
        dropna : bool, default True
            Do not include columns whose entries are all NaN. If True,
            rows with a NaN value in any column will be omitted before
            computing margins.
        margins_name : str, default ‘All’
            Name of the row / column that will contain the totals when
            margins is True.
        observed : bool, default False
            This only applies if any of the groupers are Categoricals.
            If True: only show observed values for categorical groupers.
            If False: show all values for categorical groupers.
        sort : bool, default True
            Specifies if the result should be sorted.

        Returns
        -------
        SnowflakeQueryCompiler
        """
        # TODO: SNOW-838811 observed/categorical
        if observed:
            raise NotImplementedError("Not implemented observed")

        # TODO: SNOW-838819 sort/order by
        if not sort:
            raise NotImplementedError("Not implemented not sorted")

        if columns is not None and isinstance(columns, Hashable):
            columns = [columns]

        if index is not None and isinstance(index, Hashable):
            index = [index]

        # TODO: SNOW-857485 Support for non-str and list of non-str for index/columns/values
        if index and (
            not isinstance(index, str)
            and not all([isinstance(v, str) for v in index])
            and None not in index
        ):
            raise NotImplementedError(
                f"Not implemented non-string of list of string {index}."
            )

        if values and (
            not isinstance(values, str)
            and not all([isinstance(v, str) for v in values])
            and None not in values
        ):
            raise NotImplementedError(
                f"Not implemented non-string of list of string {values}."
            )

        if columns and (
            not isinstance(columns, str)
            and not all([isinstance(v, str) for v in columns])
            and None not in columns
        ):
            raise NotImplementedError(
                f"Not implemented non-string of list of string {columns}."
            )

        if aggfunc is None or (is_list_like(aggfunc) and not all(aggfunc)):
            raise TypeError("Must provide 'func' or tuples of '(column, aggfunc).")

        if isinstance(aggfunc, dict) and (
            not all(
                [all(af if isinstance(af, list) else [af]) for af in aggfunc.values()]
            )
        ):
            raise TypeError("Must provide 'func' or named aggregation **kwargs.")

        if isinstance(aggfunc, dict) and any(
            not isinstance(af, str) for af in aggfunc.values()
        ):
            # With margins, a dictionary aggfunc that maps to list of aggregations is not supported by pandas.  We return
            # friendly error message in this case.
            if margins:
                raise ValueError(
                    "Margins not supported if list of aggregation functions"
                )
            elif index is None:
                raise NotImplementedError(
                    "Not implemented index is None and list of aggregation functions."
                )

        # Duplicate pivot column and index are not allowed, but duplicate aggregation values are supported.
        index_and_data_column_pandas_labels = (
            self._modin_frame.index_column_pandas_labels
            + self._modin_frame.data_column_pandas_labels
        )
        if columns:
            check_valid_pandas_labels(columns, index_and_data_column_pandas_labels)

        if index:
            check_valid_pandas_labels(index, index_and_data_column_pandas_labels)

        # We have checked there are no duplicates, so there will be only one matching.

        groupby_snowflake_quoted_identifiers = (
            [
                snowflake_quoted_identifier[0]
                for snowflake_quoted_identifier in self._modin_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                    index
                )
            ]
            if index
            else []
        )

        pivot_snowflake_quoted_identifiers = (
            [
                snowflake_quoted_identifier[0]
                for snowflake_quoted_identifier in self._modin_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                    columns
                )
            ]
            if columns
            else []
        )

        if values is None:
            # If no values (aggregation columns) are specified, then we use all data columns that are neither
            # groupby (index) nor pivot columns as the aggregation columns.  For example, a dataframe with
            # index=['A','B'], data=['C','E'] and if 'A' is used in groupby, and 'C' used as pivot, then 'E' would be
            # used as the values column, and unused index column 'B' would be dropped.
            full_columns_and_index = (list(columns) if columns else []) + (
                list(index) if index else []
            )
            values = self._modin_frame.data_column_pandas_labels.copy()
            for pandas_label_tuple in full_columns_and_index:
                values.remove(pandas_label_tuple)

        if is_list_like(values):
            values = list(values)

        if len(values) > 0:
            values_label_to_identifier_pairs_list = generate_pivot_aggregation_value_label_snowflake_quoted_identifier_mappings(
                values, self._modin_frame
            )
            multiple_agg_funcs_single_values = (
                is_list_like(aggfunc) and len(aggfunc) > 1
            ) and not isinstance(values, list)
            include_aggr_func_in_label = (
                len(groupby_snowflake_quoted_identifiers) != 0
                or multiple_agg_funcs_single_values
            )
            pivot_aggr_groupings = list(
                generate_single_pivot_labels(
                    values_label_to_identifier_pairs_list,
                    aggfunc,
                    len(pivot_snowflake_quoted_identifiers) > 0,
                    isinstance(values, list)
                    and (not margins or len(values) > 1)
                    and include_aggr_func_in_label,
                    sort,
                )
            )
        else:
            # If there are no values, we simply return an empty DataFrame with no columns
            # whose index is the result of grouping by the index columns. We pass
            # pivot_aggr_groupings = None to signify that case in the `pivot_helper`
            # function.
            pivot_aggr_groupings = None

        # When aggfunc is not a list, we should sort the outer level of pandas labels.
        try:
            pivotted_frame = pivot_helper(
                self._modin_frame,
                pivot_aggr_groupings,
                not dropna,
                not is_list_like(aggfunc),
                columns,
                groupby_snowflake_quoted_identifiers,
                pivot_snowflake_quoted_identifiers,
                (is_list_like(aggfunc) and len(aggfunc) > 1),
                (is_list_like(aggfunc) and len(values) > 1),
                index,
                aggfunc,
            )
        except SnowparkSQLException as e:
            # `pivot_table` is implemented on the server side via the dynamic pivot
            # feature. The dynamic pivot issues a query in order to determine
            # what the pivot values are. If there are no pivot values, and no groupby
            # columns are specified, we raise an error on the server side.
            # Error Code 1146 corresponds to the Snowflake Exception when
            # a dynamic pivot is called and there are no pivot values and no
            # groupby columns specified.
            # This error is raised eagerly, since we have a
            # describe call on the client side in order to determine the schema of the output.
            # If we hit this error, that means that we have attempted a pivot on an empty
            # DataFrame, so we catch the exception and return an empty DataFrame.
            if e.sql_error_code == 1146:
                from modin.pandas.io import from_pandas

                native_df = native_pd.DataFrame(index=self.index, columns=self.columns)
                native_df.index.names = self.index.names
                native_df.columns.names = self.columns.names
                return from_pandas(
                    native_df.pivot_table(
                        index=index,
                        values=values,
                        columns=columns,
                        margins=margins,
                        margins_name=margins_name,
                        dropna=dropna,
                        aggfunc=aggfunc,
                        fill_value=fill_value,
                        observed=observed,
                        sort=sort,
                    )
                )._query_compiler
            else:
                raise e

        pivot_qc = SnowflakeQueryCompiler(pivotted_frame)

        # If dropna, then filter out any rows that contain all null aggregation values.
        if dropna:
            pivot_qc = pivot_qc.dropna(
                axis=0, how="all", subset=pivotted_frame.data_column_pandas_labels
            )

        # If there is a fill_value then project with coalesce on the non-group by columns.
        if fill_value:
            pivot_qc = pivot_qc.fillna(fill_value, self_is_series=False)

        # Add margins if specified, note this will also add the row position since the margin row needs to be fixed
        # as the last row of the dataframe.  If no margins, then we order by the group by columns.
        # The final condition checks to see if there are any columns in the pivot result. If there are no columns,
        # this means that we pivoted on an empty table - in that case, we can skip adding margins. We may need to add
        # an additional layer to the columns Index (since margins is True), but the expand_pivot_result_with_pivot_table_margins
        # codepath will add additional joins and unions to our query that aren't necessary, since the DataFrame is empty either way.
        if (
            margins
            and pivot_aggr_groupings
            and pivot_snowflake_quoted_identifiers
            and len(pivot_qc.columns) != 0
        ):
            if len(groupby_snowflake_quoted_identifiers) > 0:
                pivot_qc = expand_pivot_result_with_pivot_table_margins(
                    pivot_aggr_groupings,
                    groupby_snowflake_quoted_identifiers,
                    pivot_snowflake_quoted_identifiers,
                    self._modin_frame.ordered_dataframe,
                    pivot_qc,
                    margins_name,
                    fill_value,
                )
            else:
                pivot_qc = (
                    expand_pivot_result_with_pivot_table_margins_no_groupby_columns(
                        pivot_qc,
                        self._modin_frame,
                        pivot_aggr_groupings,
                        dropna,
                        columns,
                        aggfunc,
                        pivot_snowflake_quoted_identifiers,
                        values,
                        margins_name,
                        aggfunc,
                    )
                )
        elif (
            margins
            and len(pivot_qc.columns) == 0
            and len(pivot_qc.columns.names) != (len(columns) + len(self.columns.names))
        ):
            # If `margins` is True, and our result is empty, the results columns must retain the names
            # from the input DataFrame's columns. One caveat is when there are no values columns - in that case
            # pandas retains the names from the input DataFrame's columns Index regardless of if margins is True or not
            # (which we handle in pivot_utils.py), so in that case, we shouldn't add the original names to the columns
            # Index for a second time.
            levels: list[list] = [[]] * (
                len(pivot_qc.columns.names) + len(self.columns.names)
            )
            codes: list[list] = levels
            pivot_qc = pivot_qc.set_columns(
                pd.MultiIndex(
                    levels=levels,
                    codes=codes,
                    names=self.columns.names + pivot_qc.columns.names,
                )
            )

        if len(pivot_qc.columns) == 0 and len(pivot_qc.columns.names) == len(
            columns
        ) + len(self.columns.names):
            pivot_qc.columns.names = self.columns.names + columns

        # Rename the data column snowflake quoted identifiers to be closer to pandas labels given we
        # may have done unwrapping of surrounding quotes, ie. so will unwrap single quotes in snowflake identifiers.
        # For example, snowflake constant string "'shi''ne'" would become "shi'ne"
        name_normalized_frame = (
            pivot_qc._modin_frame.normalize_snowflake_quoted_identifiers_with_pandas_label()
        )

        return SnowflakeQueryCompiler(name_normalized_frame)

    def take_2d_positional(
        self,
        index: Union["SnowflakeQueryCompiler", slice],
        columns: Union["SnowflakeQueryCompiler", slice, int, bool, list, AnyArrayLike],
    ) -> "SnowflakeQueryCompiler":
        """
        Index QueryCompiler with passed keys.

        Parameters
        ----------
        index : Positional indices of rows to grab.
        columns : Positional indices of columns to grab.

        Returns
        -------
        BaseQueryCompiler
            New masked QueryCompiler.
        """
        # TODO: SNOW-884220 support multiindex
        # index can only be a query compiler or slice object
        assert isinstance(index, (SnowflakeQueryCompiler, slice))

        if isinstance(index, slice):
            with_row_selector = get_frame_by_row_pos_slice_frame(
                internal_frame=self._modin_frame, key=index
            )
        else:
            with_row_selector = get_frame_by_row_pos_frame(
                internal_frame=self._modin_frame,
                key=index._modin_frame,
            )

        with_col_selector = get_frame_by_col_pos(
            internal_frame=with_row_selector,
            columns=columns,
        )

        return SnowflakeQueryCompiler(with_col_selector)

    def convert_dtypes(
        self,
        infer_objects: bool = True,
        convert_string: bool = True,
        convert_integer: bool = True,
        convert_boolean: bool = True,
        convert_floating: bool = True,
        dtype_backend: DtypeBackend = "numpy_nullable",
    ) -> None:
        """
        Convert columns to the best possible dtypes using dtypes supporting ``pd.NA``.

        Parameters
        ----------
        infer_objects : bool, default: True
            Whether object dtypes should be converted to the best possible types.
        convert_string : bool, default: True
            Whether object dtypes should be converted to ``pd.StringDtype()``.
        convert_integer : bool, default: True
            Whether, if possbile, conversion should be done to integer extension types.
        convert_boolean : bool, default: True
            Whether object dtypes should be converted to ``pd.BooleanDtype()``.
        convert_floating : bool, default: True
            Whether, if possible, conversion can be done to floating extension types.
            If `convert_integer` is also True, preference will be give to integer dtypes
            if the floats can be faithfully casted to integers.
        dtype_backend: {‘numpy_nullable’, ‘pyarrow’}, default ‘numpy_nullable’
            Back-end data type applied to the resultant DataFrame (still experimental). Snowpark
            pandas ignores this argument.

        Returns
        -------
        None
        """
        raise NotImplementedError(
            "convert_dtype is not supported in Snowpark pandas since Snowpark pandas is already using a nullable data "
            "types internally"
        )

    def get_axis_len(
        self,
        axis: int,
    ) -> int:
        """Get the length of the specified axis.

        If axis = 0, return number of rows.
        Else, return number of data columns.

        Parameters
        ----------
        axis: 0 or 1.

        Returns
        -------
        Length of the specified axis.
        """
        return self._modin_frame.num_rows if axis == 0 else len(self.columns)

    def _nunique_columns(
        self, dropna: bool, include_index: bool = False
    ) -> "SnowflakeQueryCompiler":
        """
        Helper function to compute the number of unique elements in each column.

        Parameters
        ----------
        dropna: bool
            When true, does not consider NULL values as elements.
        include_index: bool, default False
            When true, include index columns when counting the number of unique elements.

        Returns
        -------
        SnowflakeQueryCompiler
            A one-row QC with the unique counts for each column. This will always have a single
            index column with the value "unique" in its row, regardless of the levels of the
            original index. This may be dropped later if necessary.
        """
        internal_frame = self._modin_frame
        new_index_identifier = (
            internal_frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
                pandas_labels=[INDEX_LABEL],
            )[0]
        )

        if not include_index and len(self.columns) == 0:
            return SnowflakeQueryCompiler.from_pandas(
                native_pd.DataFrame([], index=["unique"], dtype=float)
            )

        def make_nunique(identifier: str, dropna: bool) -> SnowparkColumn:
            if dropna:
                # do not include null values in count
                return count_distinct(col(identifier))
            else:
                # COUNT(DISTINCT) ignores NULL values, so if there is a NULL value in the column,
                # we include it via IFF(MAX(<col> IS NULL)), 1, 0) which will return 1 if there is
                # at least one NULL contained within a column, and 0 if there are no NULL values.
                return count_distinct(col(identifier)) + iff(
                    max_(col(identifier).is_null()), 1, 0
                )

        # get a new ordered df with nunique columns
        snowflake_quoted_identifiers = (
            internal_frame.data_column_snowflake_quoted_identifiers
        )
        pandas_labels = internal_frame.data_column_pandas_labels
        if include_index:
            snowflake_quoted_identifiers = (
                internal_frame.index_column_snowflake_quoted_identifiers
                + snowflake_quoted_identifiers
            )
            pandas_labels = ["unique_index"] + internal_frame.data_column_pandas_labels
        nunique_columns = [
            make_nunique(identifier, dropna).as_(identifier)
            for identifier in snowflake_quoted_identifiers
        ]

        # since we don't compute count on the index, we need to add a column for it
        ordered_dataframe = append_columns(
            internal_frame.ordered_dataframe.agg(*nunique_columns),
            [new_index_identifier],
            [pandas_lit("unique")],
        )

        # get a new internal frame
        frame = InternalFrame.create(
            ordered_dataframe=ordered_dataframe,
            data_column_pandas_labels=pandas_labels,
            data_column_snowflake_quoted_identifiers=snowflake_quoted_identifiers,
            data_column_pandas_index_names=internal_frame.data_column_pandas_index_names,
            index_column_pandas_labels=[INDEX_LABEL],
            index_column_snowflake_quoted_identifiers=[new_index_identifier],
            data_column_types=None,  # no snowpark pandas type for nunique
            index_column_types=None,  # no snowpark pandas type for nunique
        )
        return SnowflakeQueryCompiler(frame)

    def nunique_index(self, dropna: bool) -> int:
        """
        Return number of unique elements in an Index object.

        Returns
        -------
        int : The number of unique elements.
        """
        return (
            self._nunique_columns(dropna=dropna, include_index=True)
            .to_pandas()
            .iloc[0, 0]
        )

    def nunique(
        self, axis: Axis, dropna: bool, **kwargs: Any
    ) -> "SnowflakeQueryCompiler":
        if not isinstance(dropna, bool):
            raise ValueError("dropna must be of type bool")
        # support axis=0 only where unique values per column are counted using COUNT(DISTINCT)
        # raise error for axis=1 where unique values row-wise are counted
        if axis == 1:
            ErrorMessage.not_implemented(
                "Snowpark pandas nunique API doesn't yet support axis == 1"
            )
        # Result is basically a series with the column labels as index and the distinct count as values
        # for each data column
        # frame holds rows with nunique values, but result must be a series so transpose single row
        result = self._nunique_columns(dropna).transpose_single_row()
        # Set the single column's name to MODIN_UNNAMED_SERIES_LABEL
        return result.set_columns([MODIN_UNNAMED_SERIES_LABEL])

    def unique(self) -> "SnowflakeQueryCompiler":
        """Compute unique elements for series. Preserves order of how elements are encountered. Keyword arguments are
        empty.

        Returns
        -------
        Return query compiler with unique values.
        """

        assert 1 == len(
            self._modin_frame.data_column_snowflake_quoted_identifiers
        ), "unique can be only applied to 1-D DataFrame (Series)"

        # unique is ordered in the original occurrence of the elements, which is equivalent to
        # groupby aggregation with no aggregation function, sort = False, as_index = False and
        # dropna = False.
        return self.groupby_agg(
            by=self._modin_frame.data_column_pandas_labels[0],
            agg_func={},
            axis=0,
            groupby_kwargs={"sort": False, "as_index": False, "dropna": False},
            agg_args=[],
            agg_kwargs={},
        )

    def to_numeric(
        self,
        errors: Literal["ignore", "raise", "coerce"] = "raise",
    ) -> "SnowflakeQueryCompiler":
        """
        Convert underlying data to numeric dtype.

        Args:
            errors: {"ignore", "raise", "coerce"}

        Returns:
            SnowflakeQueryCompiler: New SnowflakeQueryCompiler with converted to numeric values.
        """
        assert len(self.columns) == 1, "to_numeric only work for series"

        col_id = self._modin_frame.data_column_snowflake_quoted_identifiers[0]
        col_id_sf_type = self._modin_frame.get_snowflake_type(col_id)
        # handle unsupported types
        if isinstance(
            col_id_sf_type, (DateType, TimeType, MapType, ArrayType, BinaryType)
        ):
            if errors == "raise":
                raise TypeError(f"Invalid object type {col_id_sf_type}")
            elif errors == "coerce":
                return SnowflakeQueryCompiler(
                    self._modin_frame.update_snowflake_quoted_identifiers_with_expressions(
                        {col_id: pandas_lit(None)}
                    ).frame
                )
            elif errors == "ignore":
                return self
            else:
                raise ValueError(
                    f"invalid error value specified: {errors}"
                )  # pragma: no cover

        if isinstance(col_id_sf_type, (_NumericType, BooleanType)) and not isinstance(
            col_id_sf_type, TimedeltaType
        ):
            # no need to convert
            return self

        if errors == "ignore":
            # if any value is failed to parse, to_numeric returns the original series
            # when error = 'ignore'. This requirement is hard to implement in Snowpark
            # pandas raise error for now.
            ErrorMessage.not_implemented(
                "Snowpark pandas to_numeric API doesn't yet support errors == 'ignore'"
            )

        new_col = col(col_id)
        new_col_type_is_numeric = False
        if isinstance(col_id_sf_type, TimestampType):
            # turn those date time type to nanoseconds
            new_col = date_part("epoch_nanosecond", new_col)
            new_col_type_is_numeric = True
        elif isinstance(col_id_sf_type, TimedeltaType):
            new_col = column_astype(
                col_id, col_id_sf_type, "int64", TypeMapper.to_snowflake("int64")
            )
            new_col_type_is_numeric = True
        elif not isinstance(col_id_sf_type, StringType):
            # convert to string by default for better error message
            # e.g., "Numeric value 'apple' is not recognized"
            new_col = cast(new_col, StringType())

        if not new_col_type_is_numeric:
            # pandas.to_numeric treats empty string as np.nan but Snowflake to_double will treat it as invalid, so we
            # handle this corner case here
            new_col = iff(length(new_col) == 0, pandas_lit(None), new_col)

            # always convert to double for non-numeric types, e.g., string, because it is nontrivial to check whether
            # the values are integer only
            if errors in (None, "raise"):
                new_col = builtin("to_double")(new_col)
            else:
                # try_to_double will return NULL if conversion fails, which matches coerce behavior
                new_col = builtin("try_to_double")(new_col)

            if errors == "ignore":
                new_col = coalesce(to_variant(new_col), col(col_id))

        return SnowflakeQueryCompiler(
            self._modin_frame.update_snowflake_quoted_identifiers_with_expressions(
                {col_id: new_col}
            ).frame
        )

    def take_2d_labels(
        self,
        index: Union[
            "SnowflakeQueryCompiler", Scalar, tuple, slice, list, "pd.Index", np.ndarray
        ],
        columns: Union[
            "SnowflakeQueryCompiler", Scalar, slice, list, "pd.Index", np.ndarray
        ],
    ) -> "SnowflakeQueryCompiler":
        """
        Index QueryCompiler with passed label keys.

        Parameters
        ----------
        index : Label indices of rows to grab.
        columns : Label indices of columns to grab.

        Returns
        -------
        SnowflakeQueryCompiler
        """
        if self._modin_frame.is_multiindex(axis=0) and (
            is_scalar(index) or isinstance(index, tuple)
        ):
            # convert multiindex scalar or tuple key to tuple so get_frame_by_row_label will handle it specifically,
            # i.e., use prefix match
            if is_scalar(index):
                index = (index,)
        elif is_scalar(index):
            index = pd.Series([index])._query_compiler
        # convert list like to series
        elif is_list_like(index):
            index = pd.Series(index)
            if index.dtype == "bool":
                # boolean list like indexer is always select rows by row position
                return SnowflakeQueryCompiler(
                    get_frame_by_col_label(
                        get_frame_by_row_pos_frame(
                            internal_frame=self._modin_frame,
                            key=index._query_compiler._modin_frame,
                        ),
                        columns,
                    )
                )
            index = index._query_compiler

        return SnowflakeQueryCompiler(
            get_frame_by_col_label(
                get_frame_by_row_label(
                    internal_frame=self._modin_frame,
                    key=index._modin_frame
                    if isinstance(index, SnowflakeQueryCompiler)
                    else index,
                ),
                columns,
            )
        )

    def has_multiindex(self, axis: int = 0) -> bool:
        """
        Check if specified axis is indexed by MultiIndex.

        Parameters
        ----------
        axis : {0, 1}, default: 0
            The axis to check (0 - index, 1 - columns).

        Returns
        -------
        bool
            True if index at specified axis is MultiIndex and False otherwise.
        """
        return self._modin_frame.is_multiindex(axis=axis)

    def nlevels(self, axis: int = 0) -> int:
        """
        Integer number of levels in the index.

        Args:
            axis: the axis of the index

        Returns:
            number of levels
        """
        return self._modin_frame.num_index_levels(axis=axis)

    def isna(self) -> "SnowflakeQueryCompiler":
        """
        Check for each element of self whether it's NaN.

        Returns
        -------
        BaseQueryCompiler
            Boolean mask for self of whether an element at the corresponding
            position is NaN.
        """
        new_internal_frame = self._modin_frame.apply_snowpark_function_to_columns(
            lambda column: is_null(column)
        )
        return SnowflakeQueryCompiler(new_internal_frame)

    def notna(self) -> "SnowflakeQueryCompiler":
        """
        Check for each element of `self` whether it's existing (non-missing) value.

        Returns
        -------
        BaseQueryCompiler
            Boolean mask for `self` of whether an element at the corresponding
            position is not NaN.
        """
        new_internal_frame = self._modin_frame.apply_snowpark_function_to_columns(
            lambda column: not_(is_null(column))
        )

        return SnowflakeQueryCompiler(new_internal_frame)

    def transpose_single_row(self) -> "SnowflakeQueryCompiler":
        """
        Transposes this QueryCompiler, assumes that this QueryCompiler holds a single row. Does not explicitly
        check this is true, left to the caller to ensure this is true.
        Note that the pandas label for the result column will be lost, and set to "None".

        Returns:
            SnowflakeQueryCompiler
                Transposed new QueryCompiler object.
        """
        if len(set(self._modin_frame.cached_data_column_snowpark_pandas_types)) > 1:
            # In this case, transpose may lose types.
            self._raise_not_implemented_error_for_timedelta()

        frame = self._modin_frame

        # Handle case where the dataframe has empty columns.
        if len(frame.data_columns_index) == 0:
            return transpose_empty_df(frame)

        # This follows the same approach used in SnowflakeQueryCompiler.transpose().
        # However, as an optimization, only steps (1), (2), and (4) from the four steps described in
        # SnowflakeQueryCompiler.transpose() can be performed. The pivot operation in STEP (3) can be skipped
        # given that the QueryCompiler holds a single row.

        # STEPS (1) and (2) are both achieved using the following call.
        # STEP 1) Construct a temporary index column that contains the original index with position.
        # STEP 2) Perform an unpivot which flattens the original data columns into a single name and value rows
        # grouped by the temporary transpose index column.
        unpivot_result = prepare_and_unpivot_for_transpose(
            frame, self, is_single_row=True
        )

        # Handle fallback to pandas case.
        if isinstance(unpivot_result, SnowflakeQueryCompiler):
            return unpivot_result

        # STEP 3) The pivot operation is skipped for the single row case.

        # STEP 4) The data has been transposed, all that remains is cleaning the labels.  For the non-index column,
        # the order and name is parsed from the column name, sorted and aliased for better consistency.  For the
        # TRANSPOSE_NAME_COLUMN, the row position and index names are separated into distinct columns.  In the case
        # of a multi-level index, the index is split into a column per index.
        new_internal_frame = clean_up_transpose_result_index_and_labels(
            frame,
            unpivot_result.ordered_dataframe,
            unpivot_result.variable_name_quoted_snowflake_identifier,
            unpivot_result.object_name_quoted_snowflake_identifier,
        )

        return SnowflakeQueryCompiler(new_internal_frame)

    def transpose(self) -> "SnowflakeQueryCompiler":
        """
        Transpose this QueryCompiler.

        Returns:
            SnowflakeQueryCompiler
                Transposed new QueryCompiler object.
        """
        frame = self._modin_frame

        # Handle case where the dataframe has empty columns.
        if len(frame.data_columns_index) == 0:
            return transpose_empty_df(frame)

        # The following approach to implementing transpose relies on combining unpivot and pivot operations to flip
        # the columns into rows.  We also must explicitly maintain ordering to be consistent with pandas.  Consider
        # the following example.
        #
        # df = pd.DataFrame(data={
        #       'name': ['Alice', 'Bob', 'Bob'],
        #       'score': [9.5, 8, 9.5],
        #       'employed': [False, True, False],
        #       'kids': [0, 0, 1]})
        # df.set_index('name', inplace=True)
        #
        #       | score | employed | kids
        #  name |       |          |
        # ======|=======|==========|======
        # Alice | 9.5   | False    | 0
        # Bob   | 8.0   | True     | 0
        # Bob   | 9.5   | False    | 1
        #
        # To obtain the transpose of pandas dataframe, we go through the following steps.
        # 1) Create a single column for the index (TRANSPOSE_INDEX), this is especially needed if it is a
        # multi-level index, and also to store ordering information which would otherwise be lost during operations.
        # This table includes the dummy row added with row position = -1.
        #
        # TRANSPOSE_INDEX        | [0, "score"] | [1, "employed"] | [2, "kids"]
        # =======================|==============|=================|============
        # {"0":"Alice","row":-1} | 9.5          | False           | 0
        # {"0":"Alice","row":0}  | 9.5          | False           | 0
        # {"0":"Bob","row":1}    | 8.0          | True            | 0
        # {"0":"Bob","row":2}    | 9.5          | False           | 1
        #
        # 2) Unpivot the non-index columns, this creates a column (TRANSPOSE_NAME_COLUMN) and value
        # (TRANSPOSE_VALUE_COLUMN) containing all the non-index column values from the original dataframe.
        # In case of single-row datframes, we skip step 3 below. But we still need to simulate the format of
        # its output dataframe, so that the output of this step can be consumed by step 4.
        # For this purpose, instead of TRANSPOSE_VALUE_COLUMN, we use special column name (TRANSPOSE_VALUE_COLUMN_FOR_SINGLE_ROW),
        # which follows the pattern of the corresponding column name in step 3. We also drop the TRANSPOSE_INDEX column.
        #
        # Sample output for a multi-row dataframe
        #
        #  TRANSPOSE_INDEX       | TRANSPOSE_NAME_COLUMN   | TRANSPOSE_VALUE_COLUMN
        # =======================+=========================+=======================
        # {"0":"Alice","row":-1} | [0, "score", "wmqm"]    | 9.5
        # {"0":"Alice","row":-1} | [1, "employed", "sagn"] | false
        # {"0":"Alice","row":-1} | [2, "kids", "6sky"]     | 0
        #  {"0":"Alice","row":0} | [0, "score"]            | 9.5
        #  {"0":"Alice","row":0} | [1, "employed"]         | false
        #  {"0":"Alice","row":0} | [2, "kids"]             | 0
        #  {"0":"Bob","row":1}   | [0, "score"]            | 8.0
        #  ...
        #
        # Sample output for a single-row dataframe
        #
        # TRANSPOSE_NAME_COLUMN | TRANSPOSE_VALUE_COLUMN_FOR_SINGLE_ROW
        # ======================+======================================
        #  [0, "score"]          | 9.5
        #  [1, "employed"]       | false
        #  [2, "kids"]           | 0
        #
        # 3) Pivot the index column (TRANSPOSE_INDEX), this transposes the original index into a column index and
        # aggregate on the TRANSPOSE_VALUE_COLUMN.  This spreads out previously unpivot values under the respective
        # column index columns completing the transpose. This step is skipped for single-row datframes.
        #
        #  TRANSPOSE_NAME_COLUMN | '{"0":"Alice","row":-1}' | '{"0":"Alice","row":0}' | '{"0":"Bob","row":1}' | '{"0":"Bob","row":2}'
        # =======================+==========================+=========================+=======================+======================
        #  [0, "score"]          | 9.5                      |  9.5                    | 8.0                   | 9.5
        #  [1, "employed"]       | false                    |  false                  | true                  | false
        #  [2, "kids"]           | 0                        |  0                      | 0                     | 1
        #
        # 4) Clean up the labels and re-order to reflect their original positioning but now transposed.
        # The resulting transpose would be: df.T (note that <row_position> is internal column and 'name' is index
        # data column in this example).
        # Here the dummy row, that is converted to a column after pivot, is dropped from the final dataframe.
        #
        # <row_position> | name     | Alice | Bob  | Bob
        # ===============|==========|=======|======|======
        # 0              | score    | 9.5   | 8.0  | 9.5
        # 1              | employed | False | True | False
        # 2              | kids     | 0     | 0    | 1
        #
        # The SQL equivalent of these steps are as follows:
        #
        # --STEP (4)
        # select index_obj[0] as row_position, index_obj[1] as name, * from (
        #     select parse_json(col_name) as index_obj, * from (
        #         -- STEP (1)
        #         select cast(object_construct('row', row_position, '0', name) as varchar) as index,
        #             cast(score as varchar) as "[0, ""score""]",
        #             cast(employed as varchar) as "[1, ""employed""]",
        #             cast(kids as varchar) as "[2, ""kids""]"
        #         from df3
        #     -- STEP (2)
        #     ) unpivot(val for col_name in (
        #         "[0, ""score""]",
        #         "[1, ""employed""]",
        #         "[2, ""kids""]"
        #     ))
        # -- STEP (3)
        # ) pivot(min(val) for index in (any))
        # order by row_position;

        # STEPS (1) and (2) are both achieved using the following call.
        # STEP 1) Construct a temporary index column that contains the original index with position.
        # STEP 2) Perform an unpivot which flattens the original data columns into a single name and value rows
        # grouped by the temporary transpose index column.
        unpivot_result = prepare_and_unpivot_for_transpose(
            frame, self, is_single_row=False
        )

        # Handle fallback to pandas case.
        if isinstance(unpivot_result, SnowflakeQueryCompiler):
            return unpivot_result

        # STEP 3) Perform a dynamic pivot on the temporary transpose index column (TRANSPOSE_INDEX), as the values
        # will become the new column labels.
        # The TRANSPOSE_VALUE_COLUMN values become grouped under the remaining
        # TRANSPOSE_NAME_COLUMN values.  Since there are only unique values here we can use any simple aggregation like
        # min to reflect the same value through the pivot. The ordering is also stored in the column names which
        # is later extracted as part of final column ordering sort.
        ordered_dataframe = unpivot_result.ordered_dataframe.pivot(
            col(unpivot_result.index_snowflake_quoted_identifier),
            None,
            None,
            min_(col(unpivot_result.new_value_quoted_identifier)),
        )

        # STEP 4) The data has been transposed, all that remains is cleaning the labels.  For the non-index column,
        # the order and name is parsed from the column name, sorted and aliased for better consistency.  For the
        # TRANSPOSE_NAME_COLUMN, the row position and index names are separated into distinct columns.  In the case
        # of a multi-level index, the index is split into a column per index.
        new_internal_frame = clean_up_transpose_result_index_and_labels(
            frame,
            ordered_dataframe,
            unpivot_result.variable_name_quoted_snowflake_identifier,
            unpivot_result.object_name_quoted_snowflake_identifier,
        )

        return SnowflakeQueryCompiler(new_internal_frame)

    def invert(self) -> "SnowflakeQueryCompiler":
        """
        Apply bitwise inversion for each element of the QueryCompiler.

        Returns
        -------
        BaseQueryCompiler
            New QueryCompiler containing bitwise inversion for each value.
        """

        # use NOT to compute ~
        replace_mapping = {
            identifier: not_(col(identifier))
            for identifier in self._modin_frame.data_column_snowflake_quoted_identifiers
        }

        new_internal_frame = (
            self._modin_frame.update_snowflake_quoted_identifiers_with_expressions(
                replace_mapping
            ).frame
        )
        new_qc = self.__constructor__(new_internal_frame)
        if hasattr(self, "_shape_hint"):
            new_qc._shape_hint = self._shape_hint

        return new_qc

    def astype(
        self,
        col_dtypes_map: dict[str, Union[np.dtype, ExtensionDtype]],
        errors: Literal["raise", "ignore"] = "raise",
    ) -> "SnowflakeQueryCompiler":
        """
        Convert columns dtypes to given dtypes.

        Parameters
        ----------
        col_dtypes_map : dict
            Map for column names and new dtypes.
        errors : {'raise', 'ignore'}, default: 'raise'
            Control raising of exceptions on invalid data for provided dtype.
            - raise : allow exceptions to be raised
            - ignore : suppress exceptions. On error return original object.

        Returns
        -------
        SnowflakeQueryCompiler
            New QueryCompiler with updated dtypes.
        """
        if errors != "raise":
            ErrorMessage.not_implemented(
                f"Snowpark pandas astype API doesn't yet support errors == '{errors}'"
            )

        astype_mapping = {}
        labels = list(col_dtypes_map.keys())
        col_ids = (
            self._modin_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                labels, include_index=False
            )
        )

        data_column_snowpark_pandas_types = [
            SnowparkPandasType.get_snowpark_pandas_type_for_pandas_type(t)
            for t in col_dtypes_map.values()
        ]

        for ids, label in zip(col_ids, labels):
            for id in ids:
                to_dtype = col_dtypes_map[label]
                to_sf_type = TypeMapper.to_snowflake(to_dtype)
                from_sf_type = self._modin_frame.get_snowflake_type(id)
                if isinstance(from_sf_type, StringType) and isinstance(
                    to_sf_type, TimedeltaType
                ):
                    # Raise NotImplementedError as there is no Snowflake SQL function converting
                    # string (e.g. 1 day, 3 hours, 2 minutes) to Timedelta
                    from_dtype = self.dtypes.to_dict()[label]
                    ErrorMessage.not_implemented(
                        f"dtype {pandas_dtype(from_dtype)} cannot be converted to {pandas_dtype(to_dtype)}"
                    )
                elif is_astype_type_error(from_sf_type, to_sf_type):
                    from_dtype = self.dtypes.to_dict()[label]
                    raise TypeError(
                        f"dtype {pandas_dtype(from_dtype)} cannot be converted to {pandas_dtype(to_dtype)}"
                    )
                astype_mapping[id] = column_astype(
                    id,
                    from_sf_type,
                    to_dtype,
                    to_sf_type,
                )

        return SnowflakeQueryCompiler(
            self._modin_frame.update_snowflake_quoted_identifiers_with_expressions(
                quoted_identifier_to_column_map=astype_mapping,
                snowpark_pandas_types=data_column_snowpark_pandas_types,
            ).frame
        )

    def astype_index(
        self,
        col_dtypes_map: dict[Hashable, Union[np.dtype, ExtensionDtype]],
    ) -> "SnowflakeQueryCompiler":
        """
        Convert index columns dtypes to given dtypes.

        Parameters
        ----------
        col_dtypes_map : dict
            Map for column names and new dtypes.

        Returns
        -------
        SnowflakeQueryCompiler
            New QueryCompiler with updated dtypes.
        """
        if self.is_multiindex():
            ErrorMessage.not_implemented(
                "Snowpark pandas astype API doesn't yet support MultiIndex objects"
            )

        # Adding index columns.
        col_dtypes_curr = {}
        for column in self.get_index_names():
            col_dtypes_curr[column] = self.index.dtype

        astype_mapping = {}
        labels = list(col_dtypes_map.keys())
        col_ids = (
            self._modin_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                labels, include_index=True
            )
        )
        for ids, label in zip(col_ids, labels):
            for id in ids:
                to_dtype = col_dtypes_map[label]
                to_sf_type = TypeMapper.to_snowflake(to_dtype)
                from_dtype = col_dtypes_curr[label]
                from_sf_type = self._modin_frame.get_snowflake_type(id)
                if is_astype_type_error(from_sf_type, to_sf_type):
                    raise TypeError(
                        f"dtype {pandas_dtype(from_dtype)} cannot be converted to {pandas_dtype(to_dtype)}"
                    )
                astype_mapping[id] = column_astype(
                    id,
                    from_sf_type,
                    to_dtype,
                    to_sf_type,
                )

        return SnowflakeQueryCompiler(
            self._modin_frame.update_snowflake_quoted_identifiers_with_expressions(
                astype_mapping
            ).frame
        )

    def set_2d_labels(
        self,
        index: Union[Scalar, slice, "SnowflakeQueryCompiler"],
        columns: Union[
            "SnowflakeQueryCompiler",
            tuple,
            slice,
            list,
            "pd.Index",
            np.ndarray,
        ],
        item: Union[Scalar, AnyArrayLike, "SnowflakeQueryCompiler"],
        matching_item_columns_by_label: bool,
        matching_item_rows_by_label: bool,
        index_is_bool_indexer: bool,
        deduplicate_columns: bool = False,
        frame_is_df_and_item_is_series: bool = False,
    ) -> "SnowflakeQueryCompiler":
        """
        Create a new SnowflakeQueryCompiler with indexed columns and rows replaced by item.

        Args:
            index: labels of rows to set
            columns:  labels of columns to set
            item: new values that will be set to indexed columns and rows
            matching_item_columns_by_label: if True (e.g., df.loc[row_key, col_key] = item), only ``item``'s column labels match
                with col_key are used to set df values; otherwise, (e.g., df.loc[row_key_only] = item), use item's
                column position to match with the main frame. E.g., df has columns ["A", "B", "C"] and item has columns
                ["C", "B", "A"], df.loc[:] = item will update df's columns "A", "B", "C" using item column "C", "B", "A"
                respectively.
            matching_item_rows_by_label: if True (e.g., df.loc[row_key, col_key] = item), only ``item``'s row labels match
                with row_key are used to set df values; otherwise, (e.g., df.loc[col_key_only] = item), use item's
                row position to match with the main frame. E.g., df has rows ["A", "B", "C"] and item is a 2D NumPy Array
                df.loc[:] = item will update df's rows "A", "B", "C" using item's rows 0, 1, 2.
                respectively.
                `matching_item_rows_by_label` diverges from pandas behavior due to the lazy nature of snowpandas. In native
                pandas, if the length of the objects that we are joining is not equivalent, then pandas would error out
                because the shape is not broadcastable; while here, we use standard left join behavior.
            index_is_bool_indexer: if True, the index is a boolean indexer.
            deduplicate_columns: if True, deduplicate columns from ``columns``, e.g., if columns = ["A","A"], only the
                second "A" column will be used.
            frame_is_df_and_item_is_series: Whether item is from a Series and is being set to a DataFrame object
        Returns:
            Updated SnowflakeQueryCompiler
        """
        # TODO SNOW-962260 support multiindex
        # TODO SNOW-966481 support series
        # TODO SNOW-978570 support index or column is None
        if isinstance(index, slice):
            if index != slice(None):
                # No need to get index frame by slice if index is slice(None)
                row_frame = get_index_frame_by_row_label_slice(self._modin_frame, index)
                index = SnowflakeQueryCompiler(row_frame)

        result_frame = set_frame_2d_labels(
            internal_frame=self._modin_frame,
            index=index._modin_frame
            if isinstance(index, SnowflakeQueryCompiler)
            else index,
            columns=columns,
            item=item._modin_frame
            if isinstance(item, SnowflakeQueryCompiler)
            else item,
            matching_item_columns_by_label=matching_item_columns_by_label,
            matching_item_rows_by_label=matching_item_rows_by_label,
            index_is_bool_indexer=index_is_bool_indexer,
            deduplicate_columns=deduplicate_columns,
            frame_is_df_and_item_is_series=frame_is_df_and_item_is_series,
        )

        return SnowflakeQueryCompiler(result_frame)

    def set_2d_positional(
        self,
        index: Union["SnowflakeQueryCompiler", slice, list, tuple, Scalar],
        columns: Union["SnowflakeQueryCompiler", slice, list, tuple, Scalar],
        item: Union["SnowflakeQueryCompiler", Scalar],
        set_as_coords: bool,
        is_item_series: bool,
    ) -> "SnowflakeQueryCompiler":
        """
        Create a new SnowflakeQueryCompiler with indexed columns and rows replaced by item .
        Parameters
        ----------
        index : SnowflakeQueryCompiler
            Positional indices of rows to set.
        columns : SnowflakeQueryCompiler
            Positional indices of columns to set.
        item : new values that will be set to indexed columns and rows.
        set_as_coords: if setting (row, col) pairs as co-ordinates rather than entire row or col.
        is_item_series: if item is from a Series

        Returns
        -------
        SnowflakeQueryCompiler
        """
        row_positions_frame = get_row_pos_frame_from_row_key(index, self._modin_frame)

        column_positions = get_valid_col_pos_list_from_columns(
            columns, self.get_axis_len(1)
        )

        result_frame = set_frame_2d_positional(
            internal_frame=self._modin_frame,
            index=row_positions_frame,
            columns=column_positions,
            set_as_coords=set_as_coords,
            item=item if is_scalar(item) else item._modin_frame,
            is_item_series=is_item_series,
        )

        return SnowflakeQueryCompiler(result_frame)

    def getitem_row_array(
        self, key: Union[list[Any], "pd.Series", InternalFrame]
    ) -> "SnowflakeQueryCompiler":
        """
        Get row data for target (positional) indices.

        Parameters
        ----------
        key : list-like, Snowpark pandas Series, InternalFrame
            Numeric indices of the rows to pick.

        Returns
        -------
        SnowflakeQueryCompiler
            New QueryCompiler that contains specified rows.
        """

        from modin.pandas import Series

        # convert key to internal frame via Series
        key_frame = None
        if isinstance(key, Series):
            key_frame = key._query_compiler._modin_frame  # pragma: no cover
        elif isinstance(key, InternalFrame):
            key_frame = key  # pragma: no cover
        elif is_list_like(key):
            key_frame = Series(key)._query_compiler._modin_frame

        new_frame = get_frame_by_row_pos_frame(
            self._modin_frame, key_frame
        )  # pragma: no cover

        return SnowflakeQueryCompiler(new_frame)

    def case_when(self, caselist: List[tuple]) -> "SnowflakeQueryCompiler":
        """
        Replace values where the conditions are True.

        Args:
            caselist: A list of tuples of conditions and expected replacements
             Takes the form: (condition0, replacement0), (condition1, replacement1), … .
             condition should be a 1-D boolean array-like object or a callable.
             replacement should be a 1-D array-like object, a scalar or a callable.

        Returns:
            New QueryCompiler with replacements.
        """
        # Validate caselist. Errors raised are same as native pandas.
        if not isinstance(caselist, list):
            # modin frotnend always passes a list, but we still keep this check to guard
            # against any breaking changes in frontend layer.
            raise TypeError(
                f"The caselist argument should be a list; instead got {type(caselist)}"
            )
        if not caselist:
            raise ValueError(
                "provide at least one boolean condition, with a corresponding replacement."
            )

        # Validate entries in caselist. Errors raised are same as native pandas.
        for num, entry in enumerate(caselist):
            if not isinstance(entry, tuple):
                # modin frotnend always passes a tuple, but we still eep this check to
                # guard against any breaking changes in frontend layer.
                raise TypeError(
                    f"Argument {num} must be a tuple; instead got {type(entry)}."
                )
            if len(entry) != 2:
                raise ValueError(
                    f"Argument {num} must have length 2; "
                    "a condition and replacement; "
                    f"instead got length {len(entry)}."
                )

        orig_frame = self._modin_frame
        joined_frame = self._modin_frame
        case_expr: Optional[CaseExpr] = None
        for cond, replacement in caselist:
            if isinstance(cond, SnowflakeQueryCompiler):
                joined_frame, _ = join_utils.align_on_index(
                    joined_frame, cond._modin_frame, "left"
                )
            elif is_list_like(cond):
                cond_frame = self.from_pandas(
                    pandas.DataFrame(cond)
                )._modin_frame.ensure_row_position_column()
                joined_frame = joined_frame.ensure_row_position_column()
                joined_frame, _ = join_utils.join(
                    joined_frame,
                    cond_frame,
                    how="left",
                    left_on=[joined_frame.row_position_snowflake_quoted_identifier],
                    right_on=[cond_frame.row_position_snowflake_quoted_identifier],
                )
            elif callable(cond):
                # TODO SNOW-1489503: Add support for callable
                ErrorMessage.not_implemented(
                    "Snowpark pandas method Series.case_when doesn't yet support callable as condition"
                )
            else:
                raise TypeError(
                    f"condition must be a Series or 1-D array-like object; instead got {type(cond)}"
                )

            # if indices are misaligned treat the condition as True
            cond_expr = coalesce(
                col(joined_frame.data_column_snowflake_quoted_identifiers[-1]),
                pandas_lit(True),
            )
            if isinstance(replacement, SnowflakeQueryCompiler):
                joined_frame, _ = join_utils.align_on_index(
                    joined_frame, replacement._modin_frame, "left"
                )
                value = col(joined_frame.data_column_snowflake_quoted_identifiers[-1])
            elif is_scalar(replacement):
                value = pandas_lit(replacement)
            elif is_list_like(replacement):
                repl_frame = self.from_pandas(
                    pandas.DataFrame(replacement)
                )._modin_frame.ensure_row_position_column()
                joined_frame = joined_frame.ensure_row_position_column()
                joined_frame, _ = join_utils.join(
                    joined_frame,
                    repl_frame,
                    how="left",
                    left_on=[joined_frame.row_position_snowflake_quoted_identifier],
                    right_on=[repl_frame.row_position_snowflake_quoted_identifier],
                )
                value = col(joined_frame.data_column_snowflake_quoted_identifiers[-1])
            elif callable(replacement):
                # TODO SNOW-1489503: Add support for callable
                ErrorMessage.not_implemented(
                    "Snowpark pandas method Series.case_when doesn't yet support callable as replacement"
                )
            else:
                raise TypeError(
                    f"replacement must be a Series, 1-D array-like object or scalar; instead got {type(replacement)}"
                )

            case_expr = (
                when(cond_expr, value)
                if case_expr is None
                else case_expr.when(cond_expr, value)
            )
        orig_col = col(joined_frame.data_column_snowflake_quoted_identifiers[0])
        case_expr = orig_col if case_expr is None else case_expr.otherwise(orig_col)
        (
            joined_frame,
            _,
        ) = joined_frame.update_snowflake_quoted_identifiers_with_expressions(
            {joined_frame.data_column_snowflake_quoted_identifiers[0]: case_expr}
        )
        new_frame = InternalFrame.create(
            ordered_dataframe=joined_frame.ordered_dataframe,
            index_column_pandas_labels=orig_frame.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=joined_frame.index_column_snowflake_quoted_identifiers,
            data_column_pandas_index_names=orig_frame.data_column_pandas_index_names,
            data_column_pandas_labels=orig_frame.data_column_pandas_labels,
            data_column_snowflake_quoted_identifiers=joined_frame.data_column_snowflake_quoted_identifiers[
                :1
            ],
            data_column_types=joined_frame.cached_index_column_snowpark_pandas_types[
                :1
            ],
            index_column_types=joined_frame.cached_index_column_snowpark_pandas_types,
        )
        return SnowflakeQueryCompiler(new_frame)

    def mask(
        self,
        cond: "SnowflakeQueryCompiler",
        other: Optional[Union["SnowflakeQueryCompiler", Scalar]],
        axis: Optional[int] = None,
        level: Optional[int] = None,
        cond_fillna_with_true: bool = False,
    ) -> "SnowflakeQueryCompiler":
        """
        Replace values where the condition is True.

        Parameters
        ----------
        cond : SnowflakeQueryCompiler
            Where cond is False, keep the original value otherwise replace with corresponding value from other.

        other : Optional Scalar or SnowflakeQueryCompiler
            Entries where cond is True are replaced with corresponding value from other.  To keep things simple
            if the other is not a SnowflakeQueryCompiler or scalar primitive like int, float, str, bool then we
            raise not implemented error.

        axis : int, default None
            Alignment axis if needed.  This will raise not implemented error if not the default.

        level : int, default None
            Alignment level if needed.  This will raise not implemented error if not the default.

        needs_positional_join_for_cond : bool, default False
            Align condition and self by position rather than labels. Necessary when condition is a NumPy object.

        needs_positional_join_for_other : bool, default False
            Align other and self by position rather than labels. Necessary when other is a NumPy object.

        cond_fillna_with_true : bool, default False
            Whether this codepath is being used for setitem. If so, instead of replacing values for which
            the cond is not present (i.e. in the case that cond has fewer rows/cols than self), keep the
            original values.

        other_is_series_self_is_not : bool, default False
            Whether this codepath is being used when self is a DataFrame, and other is a Series - which
            requires parsing the axis argument.

        self_and_cond_is_series : bool, default False
            Whether this codepath is being used when both self and cond are Series - which requires matching
            the data columns regardless of label.

        Returns
        -------
        SnowflakeQueryCompiler
            New SnowflakeQueryCompiler with where result.
        """
        validate_expected_boolean_data_columns(cond._modin_frame)
        cond = cond.invert()
        return self.where(
            cond,
            other,
            axis=axis,
            level=level,
            cond_fillna_with_true=cond_fillna_with_true,
        )

    def where(
        self,
        cond: "SnowflakeQueryCompiler",
        other: Optional[Union["SnowflakeQueryCompiler", Scalar]],
        axis: Optional[int] = None,
        level: Optional[int] = None,
        cond_fillna_with_true: bool = False,
    ) -> "SnowflakeQueryCompiler":
        """
        Replace values where the condition is False.

        Parameters
        ----------
        cond : SnowflakeQueryCompiler
            Where cond is True, keep the original value otherwise replace with corresponding value from other.

        other : Optional Scalar or SnowflakeQueryCompiler
            Entries where cond is False are replaced with corresponding value from other.  To keep things simple
            if the other is not a SnowflakeQueryCompiler or scalar primitive like int, float, str, bool then we
            raise not implemented error.

        axis : int, default None
            Alignment axis if needed.  This will raise not implemented error if not the default.

        level : int, default None
            Alignment level if needed.  This will raise not implemented error if not the default.

        needs_positional_join_for_cond : bool, default False
            Align condition and self by position rather than labels. Necessary when condition is a NumPy object.

        needs_positional_join_for_other : bool, default False
            Align other and self by position rather than labels. Necessary when other is a NumPy object.

        cond_fillna_with_true : bool, default False
            Whether this codepath is being used for setitem. If so, instead of replacing values for which
            the cond is not present (i.e. in the case that cond has fewer rows/cols than self), keep the
            original values, by filling in those values with True.

        other_is_series_self_is_not : bool, default False
            Whether this codepath is being used when self is a DataFrame, and other is a Series - which
            requires parsing the axis argument.

        self_and_cond_is_series : bool, default False
            Whether this codepath is being used when both self and cond are Series - which requires matching
            the data columns regardless of label.

        Returns
        -------
        SnowflakeQueryCompiler
            New SnowflakeQueryCompiler with where result.
        """
        # Raise not implemented error if level is specified, or other is not snowflake query compiler or
        # involves more complex scalar type (not simple scalar types like int or float)
        from modin.pandas.utils import is_scalar

        other_is_series_self_is_not = (getattr(self, "_shape_hint", None) is None) and (
            getattr(other, "_shape_hint", None) == "column"
        )
        if axis is not None and not other_is_series_self_is_not:
            ErrorMessage.not_implemented(
                "Snowpark pandas where API doesn't yet support axis parameter when 'other' is Series"
            )

        if level is not None:
            ErrorMessage.not_implemented(
                "Snowpark pandas where API doesn't yet support level parameter"
            )

        if (
            other is not None
            and not isinstance(other, SnowflakeQueryCompiler)
            and not is_scalar(other)
        ):
            ErrorMessage.not_implemented(
                "Snowpark pandas where API only supports scalar, DataFrame and Series as 'other' parameter"
            )

        frame = self._modin_frame
        cond_frame = cond._modin_frame
        validate_expected_boolean_data_columns(cond_frame)

        cond_frame.validate_no_duplicated_data_columns_mapped_for_labels(
            frame.data_column_pandas_labels, "condition"
        )
        if isinstance(other, SnowflakeQueryCompiler):
            other._modin_frame.validate_no_duplicated_data_columns_mapped_for_labels(
                frame.data_column_pandas_labels, "other"
            )

        needs_positional_join_for_cond = getattr(
            cond, "_shape_hint", None
        ) == "array" or (
            getattr(self, "_shape_hint", None) is None
            and getattr(cond, "_shape_hint", None) == "column"
        )
        # align the frame and cond frame using left method
        if not needs_positional_join_for_cond:
            joined_frame, result_column_mapper = join_utils.align_on_index(
                frame,
                cond_frame,
                how="left",
            )
            mapped_frame_quoted_identifiers = (
                result_column_mapper.map_left_quoted_identifiers(
                    frame.data_column_snowflake_quoted_identifiers
                )
            )

            if (
                getattr(self, "_shape_hint", None) != "column"
                or getattr(cond, "_shape_hint", None) != "column"
            ):
                # for each data column in frame, find the column with same label in cond_frame
                # in the joined frame
                df_to_cond_identifier_mappings = (
                    get_mapping_from_left_to_right_columns_by_label(
                        frame.data_column_pandas_labels,
                        mapped_frame_quoted_identifiers,
                        cond_frame.data_column_pandas_labels,
                        result_column_mapper.map_right_quoted_identifiers(
                            cond_frame.data_column_snowflake_quoted_identifiers
                        ),
                    )
                )
            else:
                assert (
                    len(frame.data_column_snowflake_quoted_identifiers)
                    == len(cond_frame.data_column_snowflake_quoted_identifiers)
                    == 1
                ), "Series object has multiple data columns."
                # if both self and cond are series, we simply map the data columns to each other.
                df_to_cond_identifier_mappings = {
                    result_column_mapper.map_left_quoted_identifiers(
                        frame.data_column_snowflake_quoted_identifiers
                    )[0]: result_column_mapper.map_right_quoted_identifiers(
                        cond_frame.data_column_snowflake_quoted_identifiers
                    )[
                        0
                    ]
                }
        else:
            frame = frame.ensure_row_position_column()
            cond_frame = cond_frame.ensure_row_position_column()
            joined_frame, result_column_mapper = join_utils.join(
                frame,
                cond_frame,
                how="left",
                left_on=[frame.row_position_snowflake_quoted_identifier],
                right_on=[cond_frame.row_position_snowflake_quoted_identifier],
            )

            mapped_frame_quoted_identifiers = (
                result_column_mapper.map_left_quoted_identifiers(
                    frame.data_column_snowflake_quoted_identifiers
                )
            )

            # Normally, we would use label based broadcasting; however, if we have
            # made it to here in the codepath, we are either dealing with a NumPy Array
            # that has the same shape as us, or a Series. If it is a Series and axis=0,
            # there will only be one column, so we must broadcast it to all of the columns.
            if len(cond_frame.data_column_pandas_labels) != 1:
                df_to_cond_identifier_mappings = {
                    df_col: cond_col
                    for df_col, cond_col in zip(
                        mapped_frame_quoted_identifiers,
                        result_column_mapper.map_right_quoted_identifiers(
                            cond_frame.data_column_snowflake_quoted_identifiers
                        ),
                    )
                }
            else:
                cond_snowflake_quoted_identifier = (
                    result_column_mapper.map_right_quoted_identifiers(
                        cond_frame.data_column_snowflake_quoted_identifiers
                    )[0]
                )
                df_to_cond_identifier_mappings = {
                    frame_quoted_identifier: cond_snowflake_quoted_identifier
                    for frame_quoted_identifier in mapped_frame_quoted_identifiers
                }

        # When using setitem, if cond has a smaller shape than self,
        # we must fill in the missing values with True. This is a workaround
        # that is necessary for df.setitem, as default behavior for where
        # is to treat missing values as False.
        if cond_fillna_with_true:
            # Add additional rows if necessary.
            fillnone_column_map = {
                c: coalesce(c, pandas_lit(True))
                for c in df_to_cond_identifier_mappings.values()
                if c is not None
            }
            updated_results = (
                joined_frame.update_snowflake_quoted_identifiers_with_expressions(
                    fillnone_column_map
                )
            )
            joined_frame = updated_results.frame
            for k in df_to_cond_identifier_mappings.keys():
                if (
                    df_to_cond_identifier_mappings[k]
                    in updated_results.old_id_to_new_id_mappings.keys()
                ):
                    df_to_cond_identifier_mappings[
                        k
                    ] = updated_results.old_id_to_new_id_mappings[
                        df_to_cond_identifier_mappings[k]
                    ]
            # Add additional columns if necessary, and update `df_to_cond_identifier_mappings`
            # with new columns.
            updated_mappings = {}
            missing_columns = [
                df_col
                for df_col, cond_col in df_to_cond_identifier_mappings.items()
                if cond_col is None
            ]
            missing_columns += [
                col
                for col in frame.data_column_snowflake_quoted_identifiers
                if col not in df_to_cond_identifier_mappings.keys()
            ]
            for df_col in missing_columns:
                pandas_label = df_col.strip('"')
                pandas_label += "_added_col_for_setitem"
                joined_frame = joined_frame.append_column(
                    pandas_label, pandas_lit(True)
                )
                updated_mappings[
                    df_col
                ] = joined_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                    [pandas_label], include_index=False
                )[
                    0
                ][
                    0
                ]
            df_to_cond_identifier_mappings.update(updated_mappings)

        other_value = None
        needs_positional_join_for_other = getattr(other, "_shape_hint", None) == "array"
        if isinstance(other, SnowflakeQueryCompiler):
            other_frame = other._modin_frame
            if not needs_positional_join_for_other:
                if not other_is_series_self_is_not or axis == 0:
                    # align other frame with the joined_frame (frame and cond) using left method
                    joined_frame, result_column_mapper = join_utils.align_on_index(
                        joined_frame,
                        other_frame,
                        how="left",
                    )
                else:
                    other_frame = (
                        SnowflakeQueryCompiler(other_frame).transpose()._modin_frame
                    )
                    joined_frame, result_column_mapper = join_utils.join(
                        joined_frame,
                        other_frame,
                        how="cross",
                        left_on=[],
                        right_on=[],
                    )
            else:
                joined_frame = joined_frame.ensure_row_position_column()
                other_frame = other_frame.ensure_row_position_column()
                joined_frame, result_column_mapper = join_utils.join(
                    joined_frame,
                    other_frame,
                    how="left",
                    left_on=[joined_frame.row_position_snowflake_quoted_identifier],
                    right_on=[other_frame.row_position_snowflake_quoted_identifier],
                )
            # for each data column in frame, find the column with same label in other_frame
            # in the joined frame.
            mapped_frame_quoted_identifiers = (
                result_column_mapper.map_left_quoted_identifiers(
                    mapped_frame_quoted_identifiers
                )
            )
            if not needs_positional_join_for_other:
                if not (other_is_series_self_is_not and axis == 0):
                    df_to_other_identifier_mappings = (
                        get_mapping_from_left_to_right_columns_by_label(
                            frame.data_column_pandas_labels,
                            mapped_frame_quoted_identifiers,
                            other_frame.data_column_pandas_labels,
                            result_column_mapper.map_right_quoted_identifiers(
                                other_frame.data_column_snowflake_quoted_identifiers
                            ),
                        )
                    )
                else:
                    other_snowflake_quoted_identifier = (
                        result_column_mapper.map_right_quoted_identifiers(
                            other_frame.data_column_snowflake_quoted_identifiers
                        )[0]
                    )
                    df_to_other_identifier_mappings = {
                        frame_quoted_identifier: other_snowflake_quoted_identifier
                        for frame_quoted_identifier in mapped_frame_quoted_identifiers
                    }
            else:
                df_to_other_identifier_mappings = {
                    df_col: other_col
                    for df_col, other_col in zip(
                        mapped_frame_quoted_identifiers,
                        result_column_mapper.map_right_quoted_identifiers(
                            other_frame.data_column_snowflake_quoted_identifiers
                        ),
                    )
                }
        else:
            # If other is a scalar value or None, then we know the other_value directly here.
            other_value = other
            df_to_other_identifier_mappings = {}

        # record all columns needed for the final result dataframe
        where_selected_columns = []
        # select all index columns
        where_selected_columns += joined_frame.index_column_snowflake_quoted_identifiers
        # retain all ordering columns that is missing in the index columns
        missing_ordering_column_snowflake_quoted_identifiers = [
            order_col.snowflake_quoted_identifier
            for order_col in joined_frame.ordering_columns
            if order_col.snowflake_quoted_identifier not in where_selected_columns
        ]
        where_selected_columns += missing_ordering_column_snowflake_quoted_identifiers

        snowflake_quoted_identifier_to_snowflake_type = (
            joined_frame.quoted_identifier_to_snowflake_type()
        )
        new_data_column_snowflake_quoted_identifiers: list[ColumnOrName] = []
        # go over the data columns from frame in the joined_frame, and for each column it checks:
        # 1) if no matching condition column (the column in the condition frame that has same label), replace
        #    it with the other value or matched other column. If no other value of matched other column is
        #    available, replace it with lit(None).
        # 2) if there is matching condition column, replace the elements whose corresponding condition value is
        #    False with the other value or matched other column, or None if none is available.
        for pandas_label, snowflake_quoted_identifier in zip(
            frame.data_column_pandas_labels,
            mapped_frame_quoted_identifiers,
        ):
            cond_snowflake_quoted_identifier = df_to_cond_identifier_mappings.get(
                snowflake_quoted_identifier
            )
            other_snowflake_quoted_identifier = df_to_other_identifier_mappings.get(
                snowflake_quoted_identifier
            )
            col_data_type = snowflake_quoted_identifier_to_snowflake_type.get(
                snowflake_quoted_identifier
            )
            # TODO (SNOW-904421): Other value can fail to cast in snowflake if not compatible type
            if other_value is not None:
                other_col_or_literal = pandas_lit(other_value)
                other_col_data_type = infer_object_type(other_value)
                if not is_compatible_snowpark_types(other_col_data_type, col_data_type):
                    other_col_or_literal = to_variant(other_col_or_literal)
            elif other_snowflake_quoted_identifier:
                other_col_or_literal = col(other_snowflake_quoted_identifier)
                other_col_data_type = snowflake_quoted_identifier_to_snowflake_type[
                    other_snowflake_quoted_identifier
                ]
                if not is_compatible_snowpark_types(other_col_data_type, col_data_type):
                    other_col_or_literal = to_variant(other_col_or_literal)
            else:
                other_col_or_literal = pandas_lit(None)

            new_column_snowflake_quoted_identifier = (
                joined_frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
                    pandas_labels=[pandas_label],
                    excluded=new_data_column_snowflake_quoted_identifiers,
                )[0]
            )
            if cond_snowflake_quoted_identifier is None:
                where_selected_columns.append(
                    other_col_or_literal.as_(new_column_snowflake_quoted_identifier),
                )
            else:
                where_selected_columns.append(
                    iff(
                        col(cond_snowflake_quoted_identifier),
                        col(snowflake_quoted_identifier),
                        other_col_or_literal,
                    ).as_(new_column_snowflake_quoted_identifier)
                )
            new_data_column_snowflake_quoted_identifiers.append(
                new_column_snowflake_quoted_identifier
            )

        # select all column need to be selected/projected to create the final dataframe.
        where_ordered_dataframe = joined_frame.ordered_dataframe.select(
            where_selected_columns
        )
        new_frame = InternalFrame.create(
            ordered_dataframe=where_ordered_dataframe,
            data_column_pandas_labels=frame.data_column_pandas_labels,
            data_column_pandas_index_names=frame.data_column_pandas_index_names,
            data_column_snowflake_quoted_identifiers=new_data_column_snowflake_quoted_identifiers,
            index_column_pandas_labels=frame.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=joined_frame.index_column_snowflake_quoted_identifiers,
            data_column_types=frame.cached_data_column_snowpark_pandas_types,
            index_column_types=joined_frame.cached_index_column_snowpark_pandas_types,
        )
        return SnowflakeQueryCompiler(new_frame)

    def _make_fill_expression_for_column_wise_fillna(
        self,
        snowflake_quoted_identifier: str,
        method: FillNAMethod,
        limit: Optional[int] = None,
        _columns_to_ignore: Optional[list[bool]] = None,
    ) -> SnowparkColumn:
        """
        Helper function to get the Snowpark Column expression corresponding to snowflake_quoted_id when doing a column wise fillna.

        Parameters
        ----------
        snowflake_quoted_identifier : str
            The snowflake quoted identifier of the column that we are generating the expression for.
        method : FillNAMethod
            Enum representing if this method is a ffill method or a bfill method.
        limit : optional, int
            Maximum number of consecutive NA values to fill.

        Returns
        -------
        Column
            The Snowpark Column corresponding to the filled column.
        """
        method_is_ffill = method is FillNAMethod.FFILL_METHOD
        len_ids = len(self._modin_frame.data_column_snowflake_quoted_identifiers)
        # In pandas, columns are implicitly ordered. When doing a fillna on axis=1, we need to use this implicit
        # ordering in order to determine what the "previous" column is to fill values in this column.
        col_pos = self._modin_frame.data_column_snowflake_quoted_identifiers.index(
            snowflake_quoted_identifier
        )
        # If we are looking at the first column and doing an ffill, or looking at the last column and doing a bfill,
        # there are no other columns for us to coalesce with, so returning coalesce will error since it will be a
        # coalesce with one column. Instead, we just return the column.
        if (col_pos == 0 and method_is_ffill) or (
            col_pos == len_ids - 1 and not method_is_ffill
        ):
            return col(snowflake_quoted_identifier)
        if method_is_ffill:
            start_pos = 0
            if limit is not None:
                start_pos = max(col_pos - limit, start_pos)
            columns_to_include = (
                self._modin_frame.data_column_snowflake_quoted_identifiers[
                    start_pos:col_pos
                ][::-1]
            )
            if _columns_to_ignore:
                # When _colums_to_ignore is set, we are using this to perform a column-wise fill for reindex.
                # In that case, we will do two things:
                # 1. We must filter so that the only columns that appear in the coalesce are columns that
                # were previously a part of the dataframe (filter using the booleans in _columns_to_ignore).
                # 2. We must propagate NA values from existing columns (so if we call ffill, and we are filling
                # new column 'C', and old column 'A' has value 4 and old column 'B' has value NaN, we must fill
                # column 'C' with NaN, unlike with standard fillna, where we would propagate 4.)
                #
                # Now, columns_to_include includes the columns that we can use to fill the value in this column;
                # however, since we want to propagate NA values, we can find the first column in the list that is
                # an old column, and only pass that in to coalesce, so that if there is a NaN, it will be propagated,
                # and if there isn't, the correct value will be propagated.
                column = None
                for col_name, ignore_bool in zip(
                    columns_to_include, _columns_to_ignore[start_pos:col_pos][::-1]
                ):
                    if ignore_bool:
                        # This means that this is a column from the original data.
                        column = col_name
                        break
                if column is None:
                    columns_to_include = columns_to_include[0:1]
                else:
                    columns_to_include = [column]
            return coalesce(
                snowflake_quoted_identifier,
                *columns_to_include,
            )
        else:
            end_pos = len_ids
            if limit is not None:
                # Add 1 since end index is exclusive.
                end_pos = min(col_pos + limit + 1, len_ids)
            columns_to_include = (
                self._modin_frame.data_column_snowflake_quoted_identifiers[
                    col_pos:end_pos
                ]
            )
            if _columns_to_ignore:
                column = None
                for col_name, ignore_bool in zip(
                    columns_to_include, _columns_to_ignore[col_pos:end_pos]
                ):
                    if ignore_bool:
                        # This means that this is a column from the original data.
                        column = col_name
                        break
                if column is None:
                    columns_to_include = columns_to_include[0:1]
                else:
                    columns_to_include = [column]

            return coalesce(
                snowflake_quoted_identifier,
                *columns_to_include,
            )

    def fillna(
        self,
        value: Optional[Union[Hashable, Mapping, "pd.DataFrame", "pd.Series"]] = None,
        *,
        self_is_series: bool,
        method: Optional[FillnaOptions] = None,
        axis: Optional[Axis] = None,
        limit: Optional[int] = None,
        downcast: Optional[dict] = None,
    ) -> "SnowflakeQueryCompiler":
        """
        Replace NaN values using provided method.

        Parameters
        ----------
        value : scalar or dict
        method : {"backfill", "bfill", "pad", "ffill", None}
        axis : {0, 1}
        limit : int, optional
        downcast : dict, optional
        **kwargs : dict
            Serves the compatibility purpose. Does not affect the result.

        Returns
        -------
        SnowflakeQueryCompiler
            New QueryCompiler with all null values filled.
        """
        return self._fillna_with_masking(
            value=value,
            self_is_series=self_is_series,
            method=method,
            axis=axis,
            limit=limit,
            downcast=downcast,
        )

    def _fillna_with_masking(
        self,
        value: Optional[Union[Hashable, Mapping, "pd.DataFrame", "pd.Series"]] = None,
        *,
        self_is_series: bool,
        method: Optional[FillnaOptions] = None,
        axis: Optional[Axis] = None,
        limit: Optional[int] = None,
        downcast: Optional[dict] = None,
        row_mask_snowflake_quoted_identifier: Optional[str] = None,
        columns_mask: Optional[list[bool]] = None,
    ) -> "SnowflakeQueryCompiler":
        """
        Replace NaN values using provided method.

        Parameters
        ----------
        value : scalar or dict
        method : {"backfill", "bfill", "pad", "ffill", None}
        axis : {0, 1}
        limit : int, optional
        downcast : dict, optional
        row_mask_snowflake_quoted_identifier : str, optional
        columns_mask : list[bool], optional
        **kwargs : dict
            Serves the compatibility purpose. Does not affect the result.

        Returns
        -------
        SnowflakeQueryCompiler
            New QueryCompiler with all null values filled.
        """
        if value is not None and limit is not None:
            ErrorMessage.not_implemented(
                "Snowpark pandas fillna API doesn't yet support 'limit' parameter with 'value' parameter"
            )
        if downcast:
            ErrorMessage.not_implemented(
                "Snowpark pandas fillna API doesn't yet support 'downcast' parameter"
            )

        # case 1: fillna df with another df or fillna series with another series/dict
        if (self_is_series and isinstance(value, (dict, pd.Series))) or (
            not self_is_series and isinstance(value, pd.DataFrame)
        ):
            if isinstance(value, dict):
                value = pd.Series(value)
            return self.where(cond=self.notna(), other=value._query_compiler)

        # case 2: fillna with a method
        if method is not None:
            # no Snowpark pandas type change in this case
            data_column_snowpark_pandas_types = (
                self._modin_frame.cached_data_column_snowpark_pandas_types
            )
            method = FillNAMethod.get_enum_for_string_method(method)
            method_is_ffill = method is FillNAMethod.FFILL_METHOD
            if axis == 0:
                self._modin_frame = self._modin_frame.ensure_row_position_column()
                if method_is_ffill:
                    func = last_value
                    if limit is None:
                        window_start = Window.UNBOUNDED_PRECEDING
                    else:
                        window_start = -1 * limit
                    window_end = Window.CURRENT_ROW
                else:
                    func = first_value
                    window_start = Window.CURRENT_ROW
                    if limit is None:
                        window_end = Window.UNBOUNDED_FOLLOWING
                    else:
                        window_end = limit

                def fillna_expr(snowflake_quoted_id: str) -> SnowparkColumn:
                    return coalesce(
                        snowflake_quoted_id,
                        func(snowflake_quoted_id, ignore_nulls=True).over(
                            Window.order_by(
                                self._modin_frame.row_position_snowflake_quoted_identifier
                            ).rows_between(window_start, window_end)
                        ),
                    )

                if row_mask_snowflake_quoted_identifier:
                    # This is an internal argument passed in by reindex
                    # that specifies a column to filter on when doing fillna
                    # columns that this filter is True for should have their
                    # NA values ignored.
                    fillna_column_map = {
                        snowflake_quoted_id: iff(
                            col(row_mask_snowflake_quoted_identifier),
                            col(snowflake_quoted_id),
                            fillna_expr(snowflake_quoted_id),
                        )
                        for snowflake_quoted_id in self._modin_frame.data_column_snowflake_quoted_identifiers
                    }

                else:
                    fillna_column_map = {
                        snowflake_quoted_id: fillna_expr(snowflake_quoted_id)
                        for snowflake_quoted_id in self._modin_frame.data_column_snowflake_quoted_identifiers
                    }
            else:
                if columns_mask is None:
                    fillna_column_map = {
                        snowflake_quoted_id: self._make_fill_expression_for_column_wise_fillna(
                            snowflake_quoted_id,
                            method,
                            limit=limit,
                        )
                        for snowflake_quoted_id in self._modin_frame.data_column_snowflake_quoted_identifiers
                    }
                else:
                    fillna_column_map = {
                        snowflake_quoted_id: self._make_fill_expression_for_column_wise_fillna(
                            snowflake_quoted_id,
                            method,
                            limit=limit,
                            _columns_to_ignore=columns_mask,
                        )
                        for i, snowflake_quoted_id in enumerate(
                            self._modin_frame.data_column_snowflake_quoted_identifiers
                        )
                        if not columns_mask[i]
                    }
        # case 3: fillna with a mapping
        else:
            # we create a mapping from column label to the fillin value and use coalesce to implement fillna
            if axis == 1 and isinstance(value, (dict, pd.Series)):
                # same as pandas
                raise ErrorMessage.not_implemented(
                    "Currently only can fill with dict/Series column by column"
                )
            from modin.pandas.utils import is_scalar

            # prepare label_to_value_map
            if is_scalar(value):
                label_to_value_map = {label: value for label in self.columns}
            elif isinstance(value, dict):
                label_to_value_map = fillna_label_to_value_map(value, self.columns)
            else:
                # TODO: SNOW-899804 alternative way to implement this fully on backend
                assert isinstance(value, pd.Series), "invalid value type {type(value)}"
                value = value.to_pandas()
                # deduplicate and keep first mapping
                value = value[~value.index.duplicated(keep="first")].to_dict()
                label_to_value_map = fillna_label_to_value_map(value, self.columns)

            if not label_to_value_map:
                # mapping is empty
                return self

            # the rest code iterates over all labels with a fill value and for each label, create a snowpark column that
            # fill null with corresponding value using coalesce
            labels = list(label_to_value_map.keys())
            id_tuples = self._modin_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                pandas_labels=labels,
                include_index=False,
            )
            fillna_column_map = {}
            data_column_snowpark_pandas_types = []
            if columns_mask is not None:
                columns_to_ignore = itertools.compress(
                    self._modin_frame.data_column_pandas_labels,
                    columns_mask,
                )
            else:
                columns_to_ignore = []  # type:ignore [assignment]
            for label, id_tuple in zip(labels, id_tuples):
                if label not in columns_to_ignore:
                    for id in id_tuple:
                        val = label_to_value_map[label]
                        if row_mask_snowflake_quoted_identifier is None:
                            fillna_column_map[id] = coalesce(id, pandas_lit(val))
                        else:
                            fillna_column_map[id] = iff(
                                col(row_mask_snowflake_quoted_identifier),
                                col(id),
                                coalesce(id, pandas_lit(val)),
                            )
                        col_type = self._modin_frame.get_snowflake_type(id)
                        col_pandas_type = (
                            col_type
                            if isinstance(col_type, SnowparkPandasType)
                            and col_type.type_match(val)
                            else None
                        )
                        data_column_snowpark_pandas_types.append(col_pandas_type)

        return SnowflakeQueryCompiler(
            self._modin_frame.update_snowflake_quoted_identifiers_with_expressions(
                fillna_column_map, data_column_snowpark_pandas_types
            ).frame
        )

    def dropna(
        self,
        axis: int,
        how: Literal["any", "all"],
        thresh: Optional[Union[int, lib.NoDefault]] = lib.no_default,
        subset: Optional[Iterable] = None,
    ) -> "SnowflakeQueryCompiler":
        """
        Remove missing values. If 'thresh' is specified then the 'how' parameter is ignored.

        Parameters
        ----------
        axis : {0, 1}
        how : {"any", "all"}
        thresh : int
        subset : list of labels

        New QueryCompiler with null values dropped along given axis.
        """
        if axis == 1:
            ErrorMessage.not_implemented(
                "Snowpark pandas dropna API doesn't yet support axis == 1"
            )

        # reuse Snowpark Dataframe's dropna API and make sure to define subset correctly, i.e., only contain data
        # columns
        subset_data_col_ids = [
            id
            for label, id in zip(
                self._modin_frame.data_column_pandas_labels,
                self._modin_frame.data_column_snowflake_quoted_identifiers,
            )
            if subset is None or label in subset
        ]
        if thresh is lib.no_default:
            thresh = None

        return SnowflakeQueryCompiler(
            InternalFrame.create(
                ordered_dataframe=self._modin_frame.ordered_dataframe.dropna(
                    how=how, thresh=thresh, subset=subset_data_col_ids
                ),
                data_column_pandas_labels=self._modin_frame.data_column_pandas_labels,
                data_column_pandas_index_names=self._modin_frame.data_column_pandas_index_names,
                data_column_snowflake_quoted_identifiers=self._modin_frame.data_column_snowflake_quoted_identifiers,
                index_column_pandas_labels=self._modin_frame.index_column_pandas_labels,
                index_column_snowflake_quoted_identifiers=self._modin_frame.index_column_snowflake_quoted_identifiers,
                data_column_types=self._modin_frame.cached_data_column_snowpark_pandas_types,
                index_column_types=self._modin_frame.cached_index_column_snowpark_pandas_types,
            )
        )

    def set_index_names(
        self, names: list[Hashable], axis: Optional[int] = 0
    ) -> "SnowflakeQueryCompiler":
        """
        Set index names for the specified axis.

        Parameters
        ----------
        names : list
            New index names. Length must be equal to number of levels in index.
        axis : {0, 1}, default: 0
            Axis to set names along.
        """
        if axis == 1:
            return self.set_columns(self.columns.set_names(names))
        else:
            frame = self._modin_frame
            if len(names) != frame.num_index_columns:
                # Same error as native pandas.
                raise ValueError(
                    "Length of names must match number of levels in MultiIndex."
                )

            # Rename pandas labels.
            frame = InternalFrame.create(
                ordered_dataframe=frame.ordered_dataframe,
                data_column_pandas_labels=frame.data_column_pandas_labels,
                data_column_snowflake_quoted_identifiers=frame.data_column_snowflake_quoted_identifiers,
                data_column_pandas_index_names=frame.data_column_pandas_index_names,
                index_column_pandas_labels=names,
                index_column_snowflake_quoted_identifiers=frame.index_column_snowflake_quoted_identifiers,
                data_column_types=frame.cached_data_column_snowpark_pandas_types,
                index_column_types=frame.cached_index_column_snowpark_pandas_types,
            )

            return SnowflakeQueryCompiler(frame)

    def setitem(
        self,
        axis: int,
        key: IndexLabel,
        value: Union["SnowflakeQueryCompiler", list[Any], Any],
    ) -> "SnowflakeQueryCompiler":
        """
        Set the row/column defined by `key` to the `value` provided.

        Parameters
        ----------
        axis : {0, 1}
            Axis to set `value` along. 0 means across rows, 1 across columns. This may be confusing at first -
            but is original Modin logic - because axis=0 means here assigning `value` across rows, i.e. adding or replacing
            a new column. For axis=1, assigning `value` across columns this equals assigning a single, full row.
            E.g., _setitem_positional(...) in the context of iloc invokes the axis=1 case and
            df[['a', 'b']] = ... the axis=0 case.
        key : label
            Row/column label to set `value` in.
        value : BaseQueryCompiler, list-like or scalar
            Define new row/column value.

        Returns
        -------
        SnowflakeQueryCompiler
            New QueryCompiler with updated `key` value.
        """

        # raise error for axis=1 which is similar to loc functionality. Setitem for axis=1
        # should be done as part of write scenarios for .loc tracked in SNOW-812522.
        # Efficient implementation requires transpose of single-row.
        if 1 == axis:
            ErrorMessage.not_implemented(
                "Snowpark pandas setitem API doesn't yet support axis == 1"
            )

        # for axis=0, update column for key
        loc = self._modin_frame.data_column_pandas_labels.index(key)

        # list_like -> must match length for non-empty df
        if is_list_like(value):
            row_count = self.get_axis_len(axis=0)
            if 0 != row_count:
                if len(value) != row_count:
                    raise ValueError(
                        f"Length of values ({len(value)}) does not match length of index ({row_count})"
                    )

            # create series out of key and insert
            value = pd.Series(value)._query_compiler

        return self.insert(loc, key, value, True, replace=True)

    def _make_discrete_difference_expression(
        self,
        periods: int,
        column_position: int,
        axis: int,
    ) -> SnowparkPandasColumn:
        """
        Helper function to generate Columns for discrete difference.

        Parameters
        ----------
        periods : int
            Periods to shift for calculating difference, accepts negative values.
        column_position : int
            The index of the column in self._modin_frame.data_column_snowflake_quoted_identifiers
            for which to calculate the discrete difference. We use position since diff on axis=1
            will use the ordering of the columns denoted by their position to determine which column
            to compute the difference with.
        axis : int {0 or 1}
            The axis over which to compute the discrete difference.

        Returns
        -------
        SnowparkPandasColumn
            An column representing the discrete difference along the specified
            axis, with the specified period, for the column specified by
            `column_position`.
        """
        # If periods is 0, we are doing a subtraction with self (or XOR in case of bool
        # dtype). In this case, even if axis is 0, we prefer to use the col-wise code,
        # since it is more efficient to just subtract (or xor) the columns, than to
        # produce the Windows necessary for the row-wise codepath.
        if axis == 0 and periods != 0:
            snowflake_quoted_identifier = (
                self._modin_frame.data_column_snowflake_quoted_identifiers[
                    column_position
                ]
            )
            column_datatype = self._modin_frame.get_snowflake_type(
                snowflake_quoted_identifier
            )
            # When computing the discrete difference over axis=0, we are basically
            # subtracting each row from the row `periods` previous. We can achieve
            # this using lag (or lead if periods is negative), as that replicates
            # the current column, but vertically offset by periods.
            func_for_other = lead if periods < 0 else lag
            # If the column is of type bool, pandas uses XOR rather than subtraction.
            if isinstance(column_datatype, BooleanType):
                col1 = col(snowflake_quoted_identifier)
                col2 = func_for_other(
                    snowflake_quoted_identifier, offset=abs(periods)
                ).over(
                    Window.order_by(
                        self._modin_frame.ordering_column_snowflake_quoted_identifiers
                    )
                )
                return SnowparkPandasColumn(
                    snowpark_column=(col1 | col2) & (not_(col1 & col2)),
                    snowpark_pandas_type=None,
                )
            else:
                return BinaryOp.create(
                    "sub",
                    col(snowflake_quoted_identifier),
                    lambda: column_datatype,
                    func_for_other(
                        snowflake_quoted_identifier, offset=abs(periods)
                    ).over(
                        Window.order_by(
                            self._modin_frame.ordering_column_snowflake_quoted_identifiers
                        )
                    ),
                    lambda: column_datatype,
                ).compute()

        else:
            # periods is the number of columns to *go back*.
            periods *= -1
            other_column_position = column_position + periods
            # In this case, we are at a column that does not have a match, because the period
            # takes us out of bounds. pandas returns a column of NaN's, regardless of the dtype
            # of the column.
            if other_column_position < 0 or other_column_position >= len(
                self._modin_frame.data_column_snowflake_quoted_identifiers
            ):
                return SnowparkPandasColumn(
                    snowpark_column=pandas_lit(np.nan), snowpark_pandas_type=None
                )
            # In this case, we are at a column that does have a match, so we must do dtype checking
            # and then generate the expression.
            else:
                col1_snowflake_quoted_identifier = (
                    self._modin_frame.data_column_snowflake_quoted_identifiers[
                        column_position
                    ]
                )
                col2_snowflake_quoted_identifier = (
                    self._modin_frame.data_column_snowflake_quoted_identifiers[
                        other_column_position
                    ]
                )
                col1_dtype = self._modin_frame.get_snowflake_type(
                    col1_snowflake_quoted_identifier
                )
                col2_dtype = self._modin_frame.get_snowflake_type(
                    col2_snowflake_quoted_identifier
                )
                col1 = col(col1_snowflake_quoted_identifier)
                col2 = col(col2_snowflake_quoted_identifier)
                # If both columns are of type bool, pandas uses XOR rather than subtraction.
                # If only one is boolean, we cast it to an integer, and use subtraction.
                if isinstance(col1_dtype, BooleanType) and isinstance(
                    col2_dtype, BooleanType
                ):
                    return SnowparkPandasColumn(
                        (col1 | col2) & (not_(col1 & col2)), snowpark_pandas_type=None
                    )
                else:
                    if isinstance(col1_dtype, BooleanType):
                        col1 = cast(col1, IntegerType())
                    if isinstance(col2_dtype, BooleanType):
                        col2 = cast(col2, IntegerType())
                    return BinaryOp.create(
                        "sub",
                        col1,
                        lambda: col1_dtype,
                        col2,
                        lambda: col2_dtype,
                    ).compute()

    def diff(self, periods: int, axis: int) -> "SnowflakeQueryCompiler":
        """
        Find discrete difference along axis.
        Args:
            periods : int
                Periods to shift for calculating difference, accepts negative values.
            axis : int
                Take difference over rows (0) or columns (1).
        Returns:
            New SnowflakeQueryCompiler with discrete differences.
        """
        diff_label_to_value_map = {
            col_name: self._make_discrete_difference_expression(periods, col_pos, axis)
            for col_pos, col_name in enumerate(
                self._modin_frame.data_column_snowflake_quoted_identifiers
            )
        }
        # diff() between two timestamp columns on axis=1, or on a single
        # timestamp column on axis 0, will change type to timedelta.
        return SnowflakeQueryCompiler(
            self._modin_frame.update_snowflake_quoted_identifiers_with_expressions(
                quoted_identifier_to_column_map={
                    k: v.snowpark_column for k, v in diff_label_to_value_map.items()
                },
                snowpark_pandas_types=[
                    a.snowpark_pandas_type for a in diff_label_to_value_map.values()
                ],
            ).frame
        )

    def drop(
        self,
        index: Optional[Sequence[Hashable]] = None,
        columns: Optional[Sequence[Hashable]] = None,
        level: Optional[Level] = None,
        errors: Literal["raise", "ignore"] = "raise",
    ) -> "SnowflakeQueryCompiler":
        """
        Drop specified rows or columns.
        Args:
            index : list of labels, optional
              Labels of rows to drop.
            columns : list of labels, optional
              Labels of columns to drop.
            level: int or level name, optional
              For MultiIndex, level from which the labels will be removed. If 'index'
              and 'columns' both are provided. This level is applicable to both.
            errors : str, default: "raise"
              If 'ignore', suppress error and only existing labels are dropped.
        Returns:
            New SnowflakeQueryCompiler with removed data.
        """
        frame = self._modin_frame
        if index is not None:
            frame = self._drop_axis_0(index, level, errors)._modin_frame
        if columns is not None:
            if level is not None:
                level = frame.parse_levels_to_integer_levels([level], False, axis=1)[0]
            data_column_labels_to_drop = []
            missing_labels = []
            for label_to_drop in columns:
                matched_labels = []
                for label in frame.data_column_pandas_labels:
                    if label_prefix_match(label, {label_to_drop: 1}, level):
                        matched_labels.append(label)
                    elif (
                        level is None
                        and label_to_drop == tuple()
                        and frame.is_multiindex(axis=1)
                    ):
                        # Empty tuple matches with everything if column index
                        # is multi-index. This behavior is same as native pandas.
                        matched_labels.append(label)
                data_column_labels_to_drop.extend(matched_labels)
                if not matched_labels:
                    missing_labels.append(label_to_drop)

            if missing_labels and errors == "raise":
                # This error message is slightly different from native pandas.
                # Native pandas raises following variations depending on input arguments
                # KeyError: {missing_labels}
                # KeyError: labels {missing_labels} not found in axis/level
                # KeyError: {missing_labels} not found in axis/level
                # In Snowpandas we raise consistent error message.
                target = "level" if level is not None else "axis"
                raise KeyError(f"labels {missing_labels} not found in {target}")

            data_column_labels = []
            data_column_identifiers = []
            data_column_snowpark_pandas_types = []
            for label, identifiers, type in zip(
                frame.data_column_pandas_labels,
                frame.data_column_snowflake_quoted_identifiers,
                frame.cached_data_column_snowpark_pandas_types,
            ):
                if label not in data_column_labels_to_drop:
                    data_column_labels.append(label)
                    data_column_identifiers.append(identifiers)
                    data_column_snowpark_pandas_types.append(type)

            frame = InternalFrame.create(
                ordered_dataframe=frame.ordered_dataframe,
                index_column_pandas_labels=frame.index_column_pandas_labels,
                index_column_snowflake_quoted_identifiers=frame.index_column_snowflake_quoted_identifiers,
                data_column_pandas_labels=data_column_labels,
                data_column_snowflake_quoted_identifiers=data_column_identifiers,
                data_column_pandas_index_names=frame.data_column_pandas_index_names,
                data_column_types=data_column_snowpark_pandas_types,
                index_column_types=frame.cached_index_column_snowpark_pandas_types,
            )
            frame = frame.select_active_columns()

        return SnowflakeQueryCompiler(frame)

    def _drop_axis_0(
        self,
        index: Sequence[Hashable],
        level: Optional[Level] = None,
        errors: Literal["raise", "ignore"] = "raise",
    ) -> "SnowflakeQueryCompiler":
        """
        Drop specified rows from the frame.
        Args:
            index : list of labels of rows to drop
            level: int or level name, optional
              For MultiIndex, level from which the labels will be removed. If 'index'
              and 'columns' both are provided. This level is applicable to both.
            errors : str, default: "raise"
              If 'ignore', suppress error and only existing labels are dropped.
        Returns:
            New SnowflakeQueryCompiler with removed data.
        """
        frame = self._modin_frame
        if level is not None:
            level = frame.parse_levels_to_integer_levels([level], False)[0]
        # filter expression to match all the provided labels. Rows matching these
        # index labels will be dropped from frame.
        filter_exp = None
        missing_labels = []
        for label in index:
            label_filter = get_snowflake_filter_for_row_label(frame, label, level)
            if errors == "raise" and (
                label_filter is None
                # We can potentially optimize this to perform check for all the
                # labels in single sql query.
                or count_rows(frame.ordered_dataframe.filter(label_filter)) == 0
            ):
                missing_labels.append(label)
            else:
                filter_exp = (
                    label_filter if filter_exp is None else filter_exp | label_filter
                )

        if missing_labels:
            # This error message is slightly different from native pandas.
            # Native pandas raises following variations depending on input arguments
            # KeyError: {missing_labels}
            # KeyError: labels {missing_labels} not found in axis/level
            # KeyError: {missing_labels} not found in axis/level
            # In Snowpandas we raise consistent error message.
            target = "level" if level is not None else "axis"
            raise KeyError(f"labels {missing_labels} not found in {target}")

        ordered_dataframe = frame.ordered_dataframe
        if filter_exp is not None:
            ordered_dataframe = ordered_dataframe.filter(not_(filter_exp))
        frame = InternalFrame.create(
            ordered_dataframe=ordered_dataframe,
            index_column_pandas_labels=frame.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=frame.index_column_snowflake_quoted_identifiers,
            data_column_pandas_labels=frame.data_column_pandas_labels,
            data_column_snowflake_quoted_identifiers=frame.data_column_snowflake_quoted_identifiers,
            data_column_pandas_index_names=frame.data_column_pandas_index_names,
            data_column_types=frame.cached_data_column_snowpark_pandas_types,
            index_column_types=frame.cached_index_column_snowpark_pandas_types,
        )
        return SnowflakeQueryCompiler(frame)

    def columnarize(self) -> "SnowflakeQueryCompiler":
        """
        Transpose this QueryCompiler if it has a single row but multiple columns.

        This method should be called for QueryCompilers representing a Series object.

        NOTE: Columnarize is brittle, and there have been some attempts to remove it
        from upstream modin because it essentially makes a guess as to whether a
        transpose should occur or not. Mahesh made an attempt here:
           https://github.com/modin-project/modin/issues/6111

        Returns
        -------
        SnowflakeQueryCompiler
            Transposed new QueryCompiler or self.
        """
        if self._shape_hint == "column":
            return self  # pragma: no cover

        # Transpose the frame if it has a single row and not one column.
        # The modin code also checks the case when it is single row, and the row
        # is a transpose of unnamed series, it will also transpose it back,
        # len(self.index) == 1 and self.index[0] == MODIN_UNNAMED_SERIES_LABEL
        #
        # We do not have such use case in Snowpark pandas.
        #
        # Many operations (sum, count) may result in a series with a single row
        # and one column from a redeuced dimension, so each of those operations
        # may need to independently perform a transpose directly so as to not
        # depend on this function entirely. See BasePandasDataset.aggregate()
        # for an example of this.
        if len(self.columns) != 1 and self.get_axis_len(0) == 1:
            return self.transpose()

        return self

    def dt_date(self) -> "SnowflakeQueryCompiler":
        return self.dt_property("date")

    def dt_time(self) -> "SnowflakeQueryCompiler":
        return self.dt_property("time")

    def dt_timetz(self) -> "SnowflakeQueryCompiler":
        return self.dt_property("timetz")

    def dt_year(self) -> "SnowflakeQueryCompiler":
        return self.dt_property("year")

    def dt_month(self) -> "SnowflakeQueryCompiler":
        return self.dt_property("month")

    def dt_day(self) -> "SnowflakeQueryCompiler":
        return self.dt_property("day")

    def dt_hour(self) -> "SnowflakeQueryCompiler":
        return self.dt_property("hour")

    def dt_minute(self) -> "SnowflakeQueryCompiler":
        return self.dt_property("minute")

    def dt_second(self) -> "SnowflakeQueryCompiler":
        return self.dt_property("second")

    def dt_microsecond(self) -> "SnowflakeQueryCompiler":
        return self.dt_property("microsecond")

    def dt_nanosecond(self) -> "SnowflakeQueryCompiler":
        return self.dt_property("nanosecond")

    def dt_dayofweek(self) -> "SnowflakeQueryCompiler":
        return self.dt_property("dayofweek")

    def dt_isocalendar(self) -> "SnowflakeQueryCompiler":
        col_name = self.columns[0]
        year_col = self.dt_property("yearofweekiso").rename(
            columns_renamer={col_name: "year"}
        )
        week_col = self.dt_property("weekiso").rename(
            columns_renamer={col_name: "week"}
        )
        day_col = self.dt_property("dayofweekiso").rename(
            columns_renamer={col_name: "day"}
        )
        return year_col.concat(axis=1, other=[week_col, day_col])

    def dt_weekday(self) -> "SnowflakeQueryCompiler":
        return self.dt_property("weekday")

    def dt_dayofyear(self) -> "SnowflakeQueryCompiler":
        return self.dt_property("dayofyear")

    def dt_quarter(self) -> "SnowflakeQueryCompiler":
        return self.dt_property("quarter")

    def dt_is_month_start(self) -> "SnowflakeQueryCompiler":
        return self.dt_property("is_month_start")

    def dt_is_month_end(self) -> "SnowflakeQueryCompiler":
        return self.dt_property("is_month_end")

    def dt_is_quarter_start(self) -> "SnowflakeQueryCompiler":
        return self.dt_property("is_quarter_start")

    def dt_is_quarter_end(self) -> "SnowflakeQueryCompiler":
        return self.dt_property("is_quarter_end")

    def dt_is_year_start(self) -> "SnowflakeQueryCompiler":
        return self.dt_property("is_year_start")

    def dt_is_year_end(self) -> "SnowflakeQueryCompiler":
        return self.dt_property("is_year_end")

    def dt_is_leap_year(self) -> "SnowflakeQueryCompiler":
        return self.dt_property("is_leap_year")

    def dt_daysinmonth(self) -> "SnowflakeQueryCompiler":
        return self.dt_property("days_in_month")

    def dt_days_in_month(self) -> "SnowflakeQueryCompiler":
        return self.dt_property("days_in_month")

    def dt_freq(self) -> "SnowflakeQueryCompiler":
        return self.dt_property("freq")

    def dt_seconds(self) -> "SnowflakeQueryCompiler":
        return self.timedelta_property("seconds")

    def dt_days(self) -> "SnowflakeQueryCompiler":
        return self.timedelta_property("days")

    def dt_microseconds(self) -> "SnowflakeQueryCompiler":
        return self.timedelta_property("microseconds")

    def dt_nanoseconds(self) -> "SnowflakeQueryCompiler":
        return self.timedelta_property("nanoseconds")

    def dt_components(self) -> "SnowflakeQueryCompiler":
        return self.dt_property("components")

    def dt_qyear(self) -> "SnowflakeQueryCompiler":
        return self.dt_property("qyear")

    def dt_start_time(self) -> "SnowflakeQueryCompiler":
        return self.dt_property("start_time")

    def dt_end_time(self) -> "SnowflakeQueryCompiler":
        return self.dt_property("end_time")

    def dt_property(
        self, property_name: str, include_index: bool = False
    ) -> "SnowflakeQueryCompiler":
        """
        Extracts the specified date or time part from the timestamp.

        Args:
            property_name: The name of the property to extract.
            include_index: Whether to include the index columns in the operation.

        Returns:
            A new SnowflakeQueryCompiler with the specified datetime property extracted.
        """
        if not include_index:
            assert len(self.columns) == 1, "dt only works for series"
            if not self.is_datetime64_any_dtype(idx=0, is_index=False):
                raise AttributeError(
                    f"'TimedeltaProperties' object has no attribute '{property_name}'"
                )

        # mapping from the property name to the corresponding snowpark function
        dt_property_to_function_map = {
            "date": to_date,
            "time": to_time,
            "hour": hour,
            "minute": minute,
            "second": second,
            "day": dayofmonth,
            "weekiso": (lambda column: builtin("weekiso")(column)),
            "dayofweekiso": (lambda column: builtin("dayofweekiso")(column)),
            "yearofweekiso": (lambda column: builtin("yearofweekiso")(column)),
            "month": month,
            "year": year,
            "quarter": quarter,
            "dayofyear": dayofyear,
            # Use DAYOFWEEKISO for `dayofweek` so that the result doesn't
            # depend on the Snowflake session's WEEK_START parameter. Subtract
            # 1 to match pandas semantics.
            "dayofweek": (lambda column: builtin("dayofweekiso")(column) - 1),
            "weekday": (lambda column: builtin("dayofweekiso")(column) - 1),
            "microsecond": (lambda column: floor(date_part("ns", column) / 1000)),
            "nanosecond": (lambda column: date_part("ns", column) % 1000),
            "is_month_start": (
                lambda column: coalesce(dayofmonth(column) == 1, pandas_lit(False))
            ),
            # To check if it's a month end, make sure that the following day is a month start.
            "is_month_end": (
                lambda column: coalesce(
                    dayofmonth(dateadd("day", pandas_lit(1), column)) == 1,
                    pandas_lit(False),
                )
            ),
            "is_quarter_start": (
                lambda column: coalesce(
                    (dayofmonth(column) == 1)
                    & (month(column).in_(*QUARTER_START_MONTHS)),
                    pandas_lit(False),
                )
            ),
            "is_quarter_end": (
                lambda column: coalesce(
                    (dayofmonth(dateadd("day", pandas_lit(1), column)) == 1)
                    & (
                        month(dateadd("day", pandas_lit(1), column)).in_(
                            *QUARTER_START_MONTHS
                        )
                    ),
                    pandas_lit(False),
                )
            ),
            "is_year_start": (
                lambda column: coalesce(
                    (dayofmonth(column) == 1) & (month(column) == 1),
                    pandas_lit(False),
                )
            ),
            "is_year_end": (
                lambda column: coalesce(
                    (dayofmonth(column) == 31) & (month(column) == 12),
                    pandas_lit(False),
                )
            ),
            "is_leap_year": (
                lambda column: coalesce(
                    dayofmonth(
                        dateadd(
                            "day",
                            pandas_lit(1),
                            date_from_parts(year(column), 2, 28),
                        )
                    )
                    == 29,
                    pandas_lit(False),
                )
            ),
            "days_in_month": (
                lambda column: when(column.is_null(), None)
                .when(
                    month(column).in_(
                        pandas_lit(1),
                        pandas_lit(3),
                        pandas_lit(5),
                        pandas_lit(7),
                        pandas_lit(8),
                        pandas_lit(10),
                        pandas_lit(12),
                    ),
                    pandas_lit(31),
                )
                .when(
                    month(column).in_(
                        pandas_lit(4),
                        pandas_lit(6),
                        pandas_lit(9),
                        pandas_lit(11),
                    ),
                    pandas_lit(30),
                )
                .when(
                    builtin("day")(
                        dateadd(
                            "day",
                            pandas_lit(1),
                            date_from_parts(
                                year(column), pandas_lit(2), pandas_lit(28)
                            ),
                        )
                    )
                    == pandas_lit(1),
                    pandas_lit(28),
                )
                .otherwise(pandas_lit(29))
            ),
        }
        property_function = dt_property_to_function_map.get(property_name)
        if not property_function:
            class_prefix = "DatetimeIndex" if include_index else "Series.dt"
            raise ErrorMessage.not_implemented(
                f"Snowpark pandas doesn't yet support the property '{class_prefix}.{property_name}'"
            )  # pragma: no cover

        return SnowflakeQueryCompiler(
            self._modin_frame.apply_snowpark_function_to_columns(
                property_function, include_index=include_index
            )
        )

    def isin(
        self,
        values: Union[
            list[Any], np.ndarray, "SnowflakeQueryCompiler", dict[Hashable, ListLike]
        ],
    ) -> "SnowflakeQueryCompiler":  # noqa: PR02
        """
        Check for each element of `self` whether it's contained in passed `values`.
        Parameters
        ----------
        values : list-like, np.array, SnowflakeQueryCompiler or dict of pandas labels -> listlike
            Values to check elements of self in. If given as dict, match ListLike to column label given as key.
        **kwargs : dict
            Serves the compatibility purpose. Does not affect the result.
        Returns
        -------
        SnowflakeQueryCompiler
            Boolean mask for self of whether an element at the corresponding
            position is contained in `values`.
        """
        is_snowflake_query_compiler = isinstance(values, SnowflakeQueryCompiler)  # type: ignore[union-attr]
        is_series = is_snowflake_query_compiler and values.is_series_like()  # type: ignore[union-attr]

        # convert list-like values to [lit(...), ..., lit(...)] and determine type
        # which is required to produce correct isin expression using array_contains(...) below
        if isinstance(values, (list, np.ndarray)):
            values_dtype, values = convert_values_to_list_of_literals_and_return_type(
                values
            )
        elif isinstance(values, dict):
            values = {
                k: convert_values_to_list_of_literals_and_return_type(v)
                for k, v in values.items()
            }

        if isinstance(values, list):
            # Apply isin(...) expression to each column.

            # Construct directly array_contains(...) columnar expression based on scalar value from list.
            # For each cell check whether it is contained in values. Handle empty list as special case, and simply replace with False.
            # Use above helper function to generate columnar expressions.
            new_frame = self._modin_frame.update_snowflake_quoted_identifiers_with_expressions(
                {
                    quoted_identifier: scalar_isin_expression(
                        quoted_identifier,
                        values,
                        self._modin_frame.get_snowflake_type(quoted_identifier),
                        values_dtype,
                    )
                    for quoted_identifier in self._modin_frame.data_column_snowflake_quoted_identifiers
                }
            ).frame
        elif isinstance(values, dict):
            # Apply isin(...) expression to all columns with a label contained in values.keys(),
            # all others should be returned as False (preserve nulls).
            replace_dict = {
                quoted_identifier: pandas_lit(False)
                for quoted_identifier in self._modin_frame.data_column_snowflake_quoted_identifiers
            }
            # matching columns are updated based on the match from the set_frame_2d
            frame = self._modin_frame
            pairs = [
                (label, identifier)
                for label, identifier in zip(
                    frame.data_column_pandas_labels,
                    frame.data_column_snowflake_quoted_identifiers,
                )
                if label in values.keys()
            ]

            replace_dict.update(
                {
                    quoted_identifier: scalar_isin_expression(
                        quoted_identifier,
                        values[label][1],
                        self._modin_frame.get_snowflake_type(quoted_identifier),
                        values[label][0],
                    )
                    for label, quoted_identifier in pairs
                }
            )

            new_frame = frame.update_snowflake_quoted_identifiers_with_expressions(
                replace_dict
            ).frame
        else:
            assert isinstance(values, SnowflakeQueryCompiler)

            # handle special case of self being empty dataframe
            row_count = self.get_axis_len(axis=0)
            if 0 == row_count:
                # idempotent operation
                return self

            if is_series:
                new_frame = compute_isin_with_series(
                    self._modin_frame, values._modin_frame
                )
            else:
                new_frame = compute_isin_with_dataframe(
                    self._modin_frame, values._modin_frame
                )

        return SnowflakeQueryCompiler(new_frame)

    def is_multiindex(self, *, axis: int = 0) -> bool:
        """
        Returns whether the InternalFrame of SnowflakeQueryCompiler has a MultiIndex along `axis`.
        Args:
            axis: If axis=0, return whether the InternalFrame has a MultiIndex as df.index.
                If axis=1, return whether the InternalFrame has a MultiIndex as df.columns.
        """
        return self._modin_frame.is_multiindex(axis=axis)

    def abs(self) -> "SnowflakeQueryCompiler":
        # TODO(SNOW-1620415): Implement abs() for timedelta.
        self._raise_not_implemented_error_for_timedelta()
        return self.unary_op("abs")

    def negative(self) -> "SnowflakeQueryCompiler":
        # TODO(SNOW-1620415): Implement __neg__() for timedelta.
        self._raise_not_implemented_error_for_timedelta()
        return self.unary_op("__neg__")

    def unary_op(self, op: str) -> "SnowflakeQueryCompiler":
        """
        Applies a unary operation `op` on each element of the `SnowflakeQueryCompiler`.

        Parameters:
        ----------
        op : Name of unary operation.

        Returns
        -------
        SnowflakeQueryCompiler
            A new SnowflakeQueryCompiler containing the unary operation `op` applied to each value.
        """

        # mapping from the unary op to the corresponding snowpark function
        op_to_snowpark_function_map = {
            "__neg__": negate,
            "abs": abs_,
        }

        op_function = op_to_snowpark_function_map.get(op)

        if not op_function:
            raise ErrorMessage.not_implemented(
                f"The unary operation {op} is currently not supported."
            )  # pragma: no cover

        new_internal_frame = self._modin_frame.apply_snowpark_function_to_columns(
            lambda col_name: op_function(col_name)
        )

        return SnowflakeQueryCompiler(new_internal_frame)

    def _make_rank_col_for_method(
        self,
        col_ident: str,
        method: Literal["min", "first", "dense", "max", "average"],
        na_option: Literal["keep", "top", "bottom"],
        ascending: bool,
        pct: bool,
        row_val: str,
        count_val: str,
    ) -> SnowparkColumn:
        """
        Helper function to get the rank Snowpark Column for method parameters {"min", "first", "dense"} and
        na_option parameters {"keep", "top", "bottom"}.

        Parameters
        ----------
        col_ident : str
            Column quoted identifier
        method: str
            Rank method value from {"min", "first", "dense", "max", "average}
        na_option: str
            Rank na_option value from {"keep", "top", "bottom"}
        ascending: bool
            Whether the elements should be ranked in ascending order.
        pct: bool
            Whether to display the returned rankings in percentile form.
        row_val: str
            Ordering column quoted identifier to get row value
        count_val: str
            Ordering column quoted identifier to get count value
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
            order_by_list += [row_val]
        # For na_option {'keep', 'bottom'}, the rank column is calculated with the specified rank function and
        # the order by clause

        rank_col = rank_func.over(Window.order_by(order_by_list))

        if method == "max":
            rank_col = rank_col - 1 + count_val

        if method == "average":
            rank_col = (2 * rank_col - 1 + count_val) / 2

        # For na_option 'keep', if the value is null then we assign it a null rank
        if na_option == "keep":
            rank_col = when(col(col_ident).is_null(), None).otherwise(rank_col)

        if pct:
            window = Window.order_by(col_ident_value).rows_between(
                Window.unboundedPreceding, Window.unboundedFollowing
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

    def rank(
        self,
        axis: Axis = 0,
        method: Literal["average", "min", "max", "first", "dense"] = "average",
        numeric_only: bool = False,
        na_option: Literal["keep", "top", "bottom"] = "keep",
        ascending: bool = True,
        pct: bool = False,
    ) -> "SnowflakeQueryCompiler":
        """
        Compute numerical rank along the specified axis.

        By default, equal values are assigned a rank that is the average of the ranks
        of those values, this behavior can be changed via `method` parameter.

        Parameters
        ----------
        axis : {0, 1}
        method : {"average", "min", "max", "first", "dense"}
            How to rank the group of records that have the same value (i.e. break ties):
            - average: average rank of the group
            - min: lowest rank in the group
            - max: highest rank in the group
            - first: ranks assigned in order they appear in the array
            - dense: like 'min', but rank always increases by 1 between groups.
        numeric_only : bool
            For DataFrame objects, rank only numeric columns if set to True.
        na_option : {"keep", "top", "bottom"}
            How to rank NaN values:
            - keep: assign NaN rank to NaN values
            - top: assign lowest rank to NaN values
            - bottom: assign highest rank to NaN values
        ascending : bool
            Whether the elements should be ranked in ascending order.
        pct : bool
            Whether to display the returned rankings in percentile form.

        Returns
        -------
        SnowflakeQueryCompiler
            A new SnowflakeQueryCompiler of the same shape as `self`, where each element is the
            numerical rank of the corresponding value along row or column.

        Examples
        --------
        >>> df = pd.DataFrame(data={'values': [1, 2, np.nan, 2, 3, np.nan, 3]})
        >>> df
           values
        0     1.0
        1     2.0
        2     NaN
        3     2.0
        4     3.0
        5     NaN
        6     3.0
        >>> df['min'] = df['values'].rank(method='min', na_option='keep')
        >>> df['dense'] = df['values'].rank(method='dense', na_option='keep')
        >>> df['first'] = df['values'].rank(method='first', na_option='keep')
        >>> df['max'] = df['values'].rank(method='max', na_option='keep')
        >>> df['avg'] = df['values'].rank(method='average', na_option='keep')

        Result of all methods using ascending order and na_option "keep" to assign NaN rank to NaN values.
        >>> df
           values  min  dense  first  max  avg
        0     1.0  1.0    1.0    1.0  1.0  1.0
        1     2.0  2.0    2.0    2.0  3.0  2.5
        2     NaN  NaN    NaN    NaN  NaN  NaN
        3     2.0  2.0    2.0    3.0  3.0  2.5
        4     3.0  4.0    3.0    4.0  5.0  4.5
        5     NaN  NaN    NaN    NaN  NaN  NaN
        6     3.0  4.0    3.0    5.0  5.0  4.5
        >>> df = pd.DataFrame(data={'values': [1, 2, np.nan, 2, 3, np.nan, 3]})
        >>> df['min'] = df['values'].rank(method='min', na_option='top')
        >>> df['dense'] = df['values'].rank(method='dense', na_option='top')
        >>> df['first'] = df['values'].rank(method='first', na_option='top')
        >>> df['max'] = df['values'].rank(method='max', na_option='top')
        >>> df['avg'] = df['values'].rank(method='average', na_option='top')

        Result of all methods using ascending order and na_option "top" to assign lowest rank to NaN values.
        >>> df
           values  min  dense  first  max  avg
        0     1.0    3      2      3    3  3.0
        1     2.0    4      3      4    5  4.5
        2     NaN    1      1      1    2  1.5
        3     2.0    4      3      5    5  4.5
        4     3.0    6      4      6    7  6.5
        5     NaN    1      1      2    2  1.5
        6     3.0    6      4      7    7  6.5
        >>> df = pd.DataFrame(data={'values': [1, 2, np.nan, 2, 3, np.nan, 3]})
        >>> df['min'] = df['values'].rank(method='min', na_option='bottom')
        >>> df['dense'] = df['values'].rank(method='dense', na_option='bottom')
        >>> df['first'] = df['values'].rank(method='first', na_option='bottom')
        >>> df['max'] = df['values'].rank(method='max', na_option='bottom')
        >>> df['avg'] = df['values'].rank(method='average', na_option='bottom')

        Result of all methods using descending order and na_option "bottom" to assign highest rank to NaN values.
        >>> df
           values  min  dense  first  max  avg
        0     1.0    1      1      1    1  1.0
        1     2.0    2      2      2    3  2.5
        2     NaN    6      4      6    7  6.5
        3     2.0    2      2      3    3  2.5
        4     3.0    4      3      4    5  4.5
        5     NaN    6      4      7    7  6.5
        6     3.0    4      3      5    5  4.5

        """
        # Rank only works correctly on valid columns - e.g. when columns have either all
        # numeric or all string values. Mixed type columns are considered nuisance columns
        # in pandas in this case and are dropped from the final result. In Snowpark pandas, str values
        # are given the highest rank.

        if axis == 1:
            ErrorMessage.not_implemented(
                f"rank parameter axis={axis} not yet supported"
            )

        query_compiler = self
        if numeric_only:
            query_compiler = drop_non_numeric_data_columns(query_compiler, [])
        original_frame = query_compiler._modin_frame
        ordered_dataframe = original_frame.ordered_dataframe
        row_val = original_frame.ordering_column_snowflake_quoted_identifiers[0]
        rank_cols = {}
        for col_name, col_ident in zip(
            original_frame.data_column_pandas_labels,
            original_frame.data_column_snowflake_quoted_identifiers,
        ):
            count_alias = ordered_dataframe.generate_snowflake_quoted_identifiers(
                pandas_labels=["c_" + col_name]
            )[0]
            # Frame to record count of non-null values
            count_df = ordered_dataframe.select(
                col_ident,
                count("*").over(Window.partition_by(col_ident)).alias(count_alias),
            ).ensure_row_position_column()
            count_val = col(count_df.projected_column_snowflake_quoted_identifiers[1])
            rank_col = self._make_rank_col_for_method(
                col_ident, method, na_option, ascending, pct, row_val, count_val
            )
            # Selects the correct method column from rank_df to be used for new_frame
            rank_df_method = count_df.select(rank_col.alias(col_name + "_" + method))
            rank_cols[col_ident] = col(
                rank_df_method.projected_column_snowflake_quoted_identifiers[0]
            )
        new_frame = original_frame
        new_frame = new_frame.update_snowflake_quoted_identifiers_with_expressions(
            rank_cols
        ).frame

        col_list = (
            new_frame.index_column_snowflake_quoted_identifiers
            + new_frame.data_column_snowflake_quoted_identifiers
        )
        new_frame = InternalFrame.create(
            ordered_dataframe=new_frame.ordered_dataframe.select(col_list),
            index_column_pandas_labels=new_frame.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=new_frame.index_column_snowflake_quoted_identifiers,
            data_column_pandas_labels=new_frame.data_column_pandas_labels,
            data_column_snowflake_quoted_identifiers=new_frame.data_column_snowflake_quoted_identifiers,
            data_column_pandas_index_names=new_frame.data_column_pandas_index_names,
            data_column_types=new_frame.cached_data_column_snowpark_pandas_types,
            index_column_types=new_frame.cached_index_column_snowpark_pandas_types,
        )

        return SnowflakeQueryCompiler(new_frame)

    def asfreq(
        self,
        freq: str,
        method: Literal["backfill", "bfill", "pad", "ffill", None] = None,
        how: Literal["start", "end", None] = None,
        normalize: bool = False,
        fill_value: Optional[Scalar] = None,
    ) -> "SnowflakeQueryCompiler":
        """
        Convert time series to specified frequency.

        Returns the original data conformed to a new index with the specified frequency.

        If the index of this Series/DataFrame is a PeriodIndex, the new index is the result of transforming the original
        index with PeriodIndex.asfreq (so the original index will map one-to-one to the new index).

        The new index will be equivalent to pd.date_range(start, end, freq=freq) where start and end are,
        respectively, the first and last entries in the original index (see pandas.date_range()). The values
        corresponding to any timesteps in the new index which were not present in the original index will be null (NaN),
        unless a method for filling such unknowns is provided (see the method parameter below).

        The resample() method is more appropriate if an operation on each group of timesteps (such as an aggregate) is
        necessary to represent the data at the new frequency.

        Parameters
        ----------
        freq : DateOffset or str
            Frequency DateOffset or string.

        method : {'backfill', 'bfill', 'pad', 'ffill'}, default None
            Method to use for filling holes in reindexed Series (note this does not fill NaNs that already were present):
            ‘pad’ / ‘ffill’: propagate last valid observation forward to next valid
            ‘backfill’ / ‘bfill’: use NEXT valid observation to fill.

        how : {'start', 'end'}, default None
            For PeriodIndex only.

        normalize : bool, default False
            Whether to reset output index to midnight.

        fill_value : scalar, optional
            Value to use for missing values, applied during upsampling
            (note this does not fill NaNs that already were present).

        Returns
        -------
        SnowflakeQueryCompiler
            Holds an ordered frame with the result of the asfreq operation.

        Notes
        -----
        This implementation calls `resample` with the `first` aggregation. `asfreq`
        is only supported on DataFrame/Series with DatetimeIndex, and only
        the `freq` and `method` parameters are currently supported.

        Examples
        --------
        >>> index = pd.date_range('1/1/2000', periods=4, freq='min')
        >>> series = pd.Series([0.0, None, 2.0, 3.0], index=index)
        >>> df = pd.DataFrame({'s': series})
        >>> df
                               s
        2000-01-01 00:00:00  0.0
        2000-01-01 00:01:00  NaN
        2000-01-01 00:02:00  2.0
        2000-01-01 00:03:00  3.0
        >>> df.asfreq(freq='30s')
                               s
        2000-01-01 00:00:00  0.0
        2000-01-01 00:00:30  NaN
        2000-01-01 00:01:00  NaN
        2000-01-01 00:01:30  NaN
        2000-01-01 00:02:00  2.0
        2000-01-01 00:02:30  NaN
        2000-01-01 00:03:00  3.0
        >>> df.asfreq(freq='30s', method='ffill')
                               s
        2000-01-01 00:00:00  0.0
        2000-01-01 00:00:30  0.0
        2000-01-01 00:01:00  NaN
        2000-01-01 00:01:30  NaN
        2000-01-01 00:02:00  2.0
        2000-01-01 00:02:30  2.0
        2000-01-01 00:03:00  3.0
        """
        if how is not None or normalize is not False or fill_value is not None:
            ErrorMessage.not_implemented(
                "Snowpark pandas `asfreq` does not support parameters `how`, `normalize`, or `fill_value`."
            )

        _, slice_unit = rule_to_snowflake_width_and_slice_unit(freq)
        if slice_unit not in RULE_SECOND_TO_DAY:
            ErrorMessage.not_implemented(
                "Snowpark pandas `asfreq` does not yet support frequencies week, month, quarter, or year"
            )

        resample_kwargs = {
            "rule": freq,
            "axis": 0,
            "closed": None,
            "label": None,
            "convention": "start",
            "kind": None,
            "on": None,
            "level": None,
            "origin": "start_day",
            "offset": None,
            "group_keys": no_default,
        }  # pragma: no cover

        return self.resample(
            resample_kwargs=resample_kwargs,
            resample_method="first" if method is None else method,
            resample_method_args=tuple(),  # type: ignore
            resample_method_kwargs={},
            is_series=False,
        )

    # TODO (SNOW-971642): Add freq to DatetimeIndex.
    # TODO (SNOW-975031): Investigate fully lazy resample implementation
    def resample(
        self,
        resample_kwargs: dict[str, Any],
        resample_method: AggFuncType,
        resample_method_args: tuple[Any],
        resample_method_kwargs: dict[str, Any],
        is_series: bool,
    ) -> Union["SnowflakeQueryCompiler", collections.defaultdict[Hashable, list]]:
        """
        Return new SnowflakeQueryCompiler whose ordered frame holds the result of a resample operation.

        Parameters
        ----------
        resample_kwargs : Dict[str, Any]
            Keyword arguments for the resample operation.

        resample_method : AggFuncType
            Resample method called on the Snowpark pandas object.

        resample_method_args : Tuple[Any]
            Keyword arguments passed to the resample method.

        resample_method_kwargs : Dict[str, Any]
            Keyword arguments passed to the resample method.

        is_series : bool
            Whether the resample method is applied on Series or not.

        Returns
        -------
        SnowflakeQueryCompiler or collections.defaultdict[Hashable, list]
            A SnowflakeQueryCompiler that holds an ordered frame with the result of the resample operation,
            or a dictionary if resample_method is 'indices'.

        Raises
        ------
        NotImplementedError
            Raises a NotImplementedError if resample arguments are not supported by
            Snowflake's execution engine.
        """

        validate_resample_supported_by_snowflake(resample_kwargs)

        axis = resample_kwargs.get("axis", 0)
        rule = resample_kwargs.get("rule")
        on = resample_kwargs.get("on")

        # Supplying 'on' to Resampler replaces the existing index of the DataFrame with the 'on' column
        if on is not None:
            if on not in self._modin_frame.data_column_pandas_labels:
                raise KeyError(f"{on}")
            frame = self.set_index(keys=[on])._modin_frame
        else:
            frame = self._modin_frame

        if resample_method in ("var", np.var) and any(
            isinstance(t, TimedeltaType)
            for t in frame.cached_data_column_snowpark_pandas_types
        ):
            raise TypeError("timedelta64 type does not support var operations")

        snowflake_index_column_identifier = (
            get_snowflake_quoted_identifier_for_resample_index_col(frame)
        )

        slice_width, slice_unit = rule_to_snowflake_width_and_slice_unit(rule)

        start_date, end_date = compute_resample_start_and_end_date(
            frame,
            snowflake_index_column_identifier,
            rule,
        )

        if resample_method in ("ffill", "bfill"):
            expected_frame = get_expected_resample_bins_frame(
                rule, start_date, end_date
            )
            # The output frame's DatetimeIndex is identical to expected_frame's. For each date in the DatetimeIndex,
            # a single row is selected from the input frame, where its date is the closest match in time based on
            # the filling method. We perform an ASOF join to accomplish this.
            index_name = frame.index_column_pandas_labels
            output_frame = perform_asof_join_on_frame(
                expected_frame, frame, resample_method
            )
            return SnowflakeQueryCompiler(output_frame).set_index_names(index_name)
        elif resample_method in IMPLEMENTED_AGG_METHODS:
            resampled_frame = perform_resample_binning_on_frame(
                frame=frame,
                datetime_index_col_identifier=snowflake_index_column_identifier,
                start_date=start_date,
                slice_width=slice_width,
                slice_unit=slice_unit,
            )
            if resample_method == "indices":
                # Convert groupby_indices output of dict[Hashable, np.ndarray] to
                # collections.defaultdict
                result_dict = SnowflakeQueryCompiler(resampled_frame).groupby_indices(
                    by=frame.index_column_pandas_labels,
                    axis=axis,
                    groupby_kwargs=dict(),
                    values_as_np_array=False,
                )
                return collections.defaultdict(list, result_dict)  # type: ignore
            elif resample_method == "size":
                # Call groupby_size directly on the dataframe or series with the index reset
                # to ensure we perform count aggregation on row positions which cannot be null
                qc = (
                    SnowflakeQueryCompiler(resampled_frame)
                    .reset_index()
                    .groupby_size(
                        by=on if on is not None else "index",
                        axis=axis,
                        groupby_kwargs=dict(),
                        agg_args=resample_method_args,
                        agg_kwargs=resample_method_kwargs,
                    )
                    .set_index_names(frame.index_column_pandas_labels)
                )
            elif resample_method in ("first", "last"):
                # Call groupby_first or groupby_last directly
                qc = getattr(
                    SnowflakeQueryCompiler(resampled_frame),
                    f"groupby_{resample_method}",
                )(
                    by=frame.index_column_pandas_labels,
                    axis=axis,
                    groupby_kwargs=dict(),
                    agg_args=resample_method_args,
                    agg_kwargs=resample_method_kwargs,
                )
            else:
                qc = SnowflakeQueryCompiler(resampled_frame).groupby_agg(
                    by=frame.index_column_pandas_labels,
                    agg_func=resample_method,
                    axis=axis,
                    groupby_kwargs=dict(),
                    agg_args=resample_method_args,
                    agg_kwargs=resample_method_kwargs,
                    numeric_only=resample_method_kwargs.get("numeric_only", False),
                    is_series_groupby=is_series,
                )

            resampled_frame_all_bins = fill_missing_resample_bins_for_frame(
                qc._modin_frame, rule, start_date, end_date
            )
            if resample_method in ("sum", "count", "size", "nunique"):
                values_arg: Union[int, dict]
                if resample_method == "sum":
                    # For sum(), we need to fill NaN values as Timedelta(0)
                    # for timedelta columns and as 0 for other columns.
                    values_arg = {}
                    for (
                        pandas_label
                    ) in resampled_frame_all_bins.data_column_pandas_labels:
                        label_dtypes: native_pd.Series = self.dtypes[[pandas_label]]
                        # query compiler's fillna() takes a dictionary mapping
                        # pandas labels to values. When we have two columns
                        # with the same pandas label and different dtypes, we
                        # may have to specify different fill values for each
                        # column, but the fillna() interface won't let us do
                        # that. Fall back to 0 in that case.
                        values_arg[pandas_label] = (
                            native_pd.Timedelta(0)
                            if len(set(label_dtypes)) == 1
                            and is_timedelta64_dtype(label_dtypes.iloc[0])
                            else 0
                        )
                    if is_series:
                        # For series, fillna() can't handle a dictionary, but
                        # there should only be one column, so pass a scalar fill
                        # value.
                        assert len(values_arg) == 1
                        values_arg = list(values_arg.values())[0]
                else:
                    values_arg = 0
                return SnowflakeQueryCompiler(resampled_frame_all_bins).fillna(
                    value=values_arg, self_is_series=is_series
                )
        else:
            ErrorMessage.not_implemented(
                f"Resample Method {resample_method} has not been implemented."
            )

        return SnowflakeQueryCompiler(resampled_frame_all_bins)

    def value_counts_index(
        self,
        normalize: bool = False,
        sort: bool = True,
        ascending: bool = False,
        bins: Optional[int] = None,
        dropna: bool = True,
    ) -> "SnowflakeQueryCompiler":
        """
        Counts the frequency or number of unique values of Index SnowflakeQueryCompiler.

        The resulting object will be in descending order so that the
        first element is the most frequently occurring element.
        Excludes NA values by default.

        Args:
            normalize : bool, default False
                If True then the object returned will contain the relative
                frequencies of the unique values.
            sort : bool, default True
                Sort by frequencies when True. Preserve the order of the data when False.
            ascending : bool, default False
                Sort in ascending order.
            bins : int, optional
                Rather than count values, group them into half-open bins,
                a convenience for ``pd.cut``, only works with numeric data.
                This argument is not supported yet.
            dropna : bool, default True
                Don't include counts of NaN.
        """
        if bins is not None:
            raise ErrorMessage.not_implemented("bins argument is not yet supported")

        assert (
            not self.is_multiindex()
        ), "value_counts_index only supports single index objects"
        by = self._modin_frame.index_column_pandas_labels
        return self._value_counts_groupby(by, normalize, sort, ascending, dropna)

    def value_counts(
        self,
        subset: Optional[Sequence[Hashable]] = None,
        normalize: bool = False,
        sort: bool = True,
        ascending: bool = False,
        bins: Optional[int] = None,
        dropna: bool = True,
    ) -> "SnowflakeQueryCompiler":
        """
        Counts the frequency or number of unique values of SnowflakeQueryCompiler.

        The resulting object will be in descending order so that the
        first element is the most frequently occurring element.
        Excludes NA values by default.

        Args:
            subset : label or list of labels, optional
                Columns to use when counting unique combinations.
            normalize : bool, default False
                If True then the object returned will contain the relative
                frequencies of the unique values.
            sort : bool, default True
                Sort by frequencies when True. Preserve the order of the data when False.
            ascending : bool, default False
                Sort in ascending order.
            bins : int, optional
                Rather than count values, group them into half-open bins,
                a convenience for ``pd.cut``, only works with numeric data.
                This argument is not supported yet.
            dropna : bool, default True
                Don't include counts of NaN.
        """
        # TODO: SNOW-924742 Support bins in Series.value_counts
        if bins is not None:
            raise ErrorMessage.not_implemented("bins argument is not yet supported")

        if subset is not None:
            if not isinstance(subset, (list, tuple)):
                subset = [subset]
            by = subset
        else:
            by = self._modin_frame.data_column_pandas_labels

        return self._value_counts_groupby(by, normalize, sort, ascending, dropna)

    def _value_counts_groupby(
        self,
        by: Sequence[Hashable],
        normalize: bool,
        sort: bool,
        ascending: bool,
        dropna: bool,
        *,
        normalize_within_groups: Optional[list[str]] = None,
    ) -> "SnowflakeQueryCompiler":
        """
        Helper method to obtain the frequency or number of unique values
        within a group.

        The resulting object will be in descending order so that the
        first element is the most frequently occurring element.
        Excludes NA values by default.

        Args:
            by : list
                Columns to perform value_counts on.
            normalize : bool
                If True then the object returned will contain the relative
                frequencies of the unique values.
            sort : bool
                Sort by frequencies when True. Preserve the order of the data when False.
            ascending : bool
                Sort in ascending order.
            dropna : bool
                Don't include counts of NaN.
            normalize_within_groups : list[str], optional
                If set, the normalize parameter will normalize based on the specified groups
                rather than the entire dataset. This parameter is exclusive to the Snowpark pandas
                query compiler and is only used internally to implement groupby_value_counts.
        """
        # validate whether by is valid (e.g., contains duplicates or non-existing labels)
        self.validate_groupby(by=by, axis=0, level=None)

        # append a dummy column for count aggregation
        COUNT_LABEL = "value_count"
        query_compiler = SnowflakeQueryCompiler(
            self._modin_frame.append_column(COUNT_LABEL, pandas_lit(1))
        )

        # count
        query_compiler = query_compiler.groupby_agg(
            by=by,
            agg_func={COUNT_LABEL: "count"},
            axis=0,
            groupby_kwargs={"dropna": dropna, "sort": False},
            agg_args=(),
            agg_kwargs={},
        )
        internal_frame = query_compiler._modin_frame
        count_identifier = internal_frame.data_column_snowflake_quoted_identifiers[0]

        # use ratio_to_report function to calculate the percentage
        # for example, if the frequencies of unique values are [2, 1, 1],
        # they are normalized to percentages as [2/(2+1+1), 1/(2+1+1), 1/(2+1+1)] = [0.5, 0.25, 0.25]
        # by default, ratio_to_report returns a decimal column, whereas pandas returns a float column
        if normalize:
            if normalize_within_groups:
                # If normalize_within_groups is set, then the denominator for ratio_to_report should
                # be the size of each group instead.
                normalize_snowflake_quoted_identifiers = [
                    entry[0]
                    for entry in internal_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                        normalize_within_groups
                    )
                ]
                window = Window.partition_by(normalize_snowflake_quoted_identifiers)
            else:
                window = None
            internal_frame = query_compiler._modin_frame.project_columns(
                [COUNT_LABEL],
                builtin("ratio_to_report")(col(count_identifier)).over(window),
            )
            count_identifier = internal_frame.data_column_snowflake_quoted_identifiers[
                0
            ]
        internal_frame = internal_frame.ensure_row_position_column()

        # When sort=True, sort by the frequency (count column);
        # otherwise, respect the original order (use the original ordering columns)
        ordered_dataframe = internal_frame.ordered_dataframe
        if sort:
            # Need to explicitly specify the row position identifier to enforce the original order.
            ordered_dataframe = ordered_dataframe.sort(
                OrderingColumn(count_identifier, ascending=ascending),
                OrderingColumn(
                    internal_frame.row_position_snowflake_quoted_identifier,
                    ascending=True,
                ),
            )

        return SnowflakeQueryCompiler(
            InternalFrame.create(
                ordered_dataframe=ordered_dataframe,
                index_column_pandas_labels=internal_frame.index_column_pandas_labels,
                index_column_snowflake_quoted_identifiers=internal_frame.index_column_snowflake_quoted_identifiers,
                # The result series of value_counts doesn't have a name, so set
                # data_column_pandas_labels to [MODIN_UNNAMED_SERIES_LABEL]
                # After pandas 2.0, it has a name `count` or `proportion`
                data_column_pandas_labels=[MODIN_UNNAMED_SERIES_LABEL],
                data_column_snowflake_quoted_identifiers=[count_identifier],
                data_column_pandas_index_names=query_compiler._modin_frame.data_column_pandas_index_names,
                data_column_types=[None],
                index_column_types=internal_frame.cached_index_column_snowpark_pandas_types,
            )
        )

    def build_repr_df(
        self,
        num_rows_to_display: int,
        num_cols_to_display: int,
        times_symbol: str = "×",
    ) -> tuple[int, int, pandas.DataFrame]:
        """
        Build pandas DataFrame for string representation.

        Parameters
        ----------
        num_rows_to_display : int
            Number of rows to show in string representation. If number of
            rows in this dataset is greater than `num_rows` then half of
            `num_rows` rows from the beginning and half of `num_rows` rows
            from the end are shown.
        num_cols_to_display : int
            Number of columns to show in string representation. If number of
            columns in this dataset is greater than `num_cols` then half of
            `num_cols` columns from the beginning and half of `num_cols`
            columns from the end are shown.
        times_symbol : str
            Symbol to use when breaking up DataFrame display to show number of rows x number of columns. Should be '×'
            for HTML mode and 'x' for repr mode

        Returns
        -------
        Tuple of row_count, col_count, pandas.DataFrame or pandas.Series
            `row_count` holds the number of rows the DataFrame has, `col_count` the number of columns the DataFrame has, and
            the pandas dataset with `num_rows` or fewer rows and `num_cols` or fewer columns.
        """
        # In order to issue less queries, use following trick:
        # 1. add the row count column holding COUNT(*) OVER () over the snowpark dataframe
        # 2. retrieve all columns
        # 3. filter on rows with recursive count

        # Previously, 2 queries were issued, and a first version replaced them with a single query and a join
        # the solution here uses a window function. This may lead to perf regressions, track these here SNOW-984177.
        # Ensure that our reference to self._modin_frame is updated with cached row count and position.
        self._modin_frame = (
            self._modin_frame.ensure_row_position_column().ensure_row_count_column()
        )
        row_count_pandas_label = (
            ROW_COUNT_COLUMN_LABEL
            if len(self._modin_frame.data_column_pandas_index_names) == 1
            else (ROW_COUNT_COLUMN_LABEL,)
            * len(self._modin_frame.data_column_pandas_index_names)
        )
        frame_with_row_count_and_position = InternalFrame.create(
            ordered_dataframe=self._modin_frame.ordered_dataframe,
            data_column_pandas_labels=self._modin_frame.data_column_pandas_labels
            + [row_count_pandas_label],
            data_column_snowflake_quoted_identifiers=self._modin_frame.data_column_snowflake_quoted_identifiers
            + [self._modin_frame.row_count_snowflake_quoted_identifier],
            data_column_pandas_index_names=self._modin_frame.data_column_pandas_index_names,
            index_column_pandas_labels=self._modin_frame.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=self._modin_frame.index_column_snowflake_quoted_identifiers,
            data_column_types=self._modin_frame.cached_data_column_snowpark_pandas_types
            + [None],
            index_column_types=self._modin_frame.cached_index_column_snowpark_pandas_types,
        )

        row_count_identifier = (
            frame_with_row_count_and_position.row_count_snowflake_quoted_identifier
        )
        row_position_snowflake_quoted_identifier = (
            frame_with_row_count_and_position.row_position_snowflake_quoted_identifier
        )

        # filter frame based on num_rows.
        # always return all columns as this may also result in a query.
        # in the future could analyze plan to see whether retrieving column count would trigger a query, if not
        # simply filter out based on static schema
        num_rows_for_head_and_tail = num_rows_to_display // 2 + 1
        new_frame = frame_with_row_count_and_position.filter(
            (
                col(row_position_snowflake_quoted_identifier)
                <= num_rows_for_head_and_tail
            )
            | (
                col(row_position_snowflake_quoted_identifier)
                >= col(row_count_identifier) - num_rows_for_head_and_tail
            )
        )

        # retrieve frame as pandas object
        new_qc = SnowflakeQueryCompiler(new_frame)
        pandas_frame = new_qc.to_pandas()

        # remove last column after first retrieving row count
        row_count = 0 if 0 == len(pandas_frame) else pandas_frame.iat[0, -1]
        pandas_frame = pandas_frame.iloc[:, :-1]
        col_count = len(pandas_frame.columns)

        return row_count, col_count, pandas_frame

    def quantiles_along_axis0(
        self,
        q: list[float],
        numeric_only: bool,
        interpolation: Literal[
            "linear", "lower", "higher", "midpoint", "nearest"
        ] = "linear",
        method: Literal["single", "table"] = "single",
        index: Optional[Union[list[str], list[float]]] = None,
        index_dtype: npt.DTypeLike = float,
    ) -> "SnowflakeQueryCompiler":
        """
        Returns values at the given quantiles for each column.

        Parameters
        ----------
        q: List[float]
            A list of quantiles to compute. These will be the row labels of the output. Snowpark Pandas supports at most
            MAX_QUANTILES_SUPPORTED (default: 16).
        numeric_only: bool
            Include only float, int, or boolean data.
        interpolation: {"linear", "lower", "higher", "midpoint", "nearest"}
            The interpolation method to use when the desired quantile lies between two data points in
            a column. Because Snowflake's PERCENTILE_CONT function performs linear interpolation and
            PERCENTILE_DISC finds the nearest value instead of interpolating, we only support those two arguments.
        method: {"single", "table"}
            When "single", computes percentiles against values within the column; when "table", computes
            against values in the whole table. Currently, only "single" is supported.
        index: Optional[List[str]], default: None
            When specified, sets the index column of the result to be this list. This is not part of
            the pandas API for quantile, and only used to implement df.describe().
            When unspecified, the index is the float values of the quantiles.
        index_dtype: npt.DTypeLike, default: float
            When specified along with ``index``, determines the type of the index column. This is only used
            for the single-column case, where index values must be coerced to strings to support an UNPIVOT,
            and otherwise is inferred. As with ``index``, this is not part of the public API, and only specified
            by ``describe``.

        Returns
        -------
        SnowflakeQueryCompiler
            A query compiler representing a DataFrame, where the columns correspond to the columns of
            the original frame, and each row has the value of the quantile for the corresponding column.
            The resulting rows are match the order that they were specified in `q`.
        """

        if len(q) > MAX_QUANTILES_SUPPORTED:
            # TODO: SNOW-1229442 Remove this code here and fix for large amount of quantiles.
            # Implementation below uses UNION ALL. This results in a high query depth causing the query analyzer to
            # produce a max recursion limit exceeded exception. Limit here to ensure performance.
            ErrorMessage.not_implemented(
                f"Snowpark pandas API supports at most {MAX_QUANTILES_SUPPORTED} quantiles."
            )

        query_compiler = self
        if numeric_only:
            query_compiler = drop_non_numeric_data_columns(query_compiler, [])
        if query_compiler.dtypes.apply(is_datetime64_any_dtype).any():
            # TODO SNOW-1003587
            ErrorMessage.not_implemented(
                "quantile is not supported for datetime columns"
            )
        assert index is None or len(index) == len(
            q
        ), f"length of index {index} did not match quantiles {q}"
        # If the index is unspecified, then use the quantiles as the index
        index_values = q if index is None else index
        if len(query_compiler._modin_frame.data_column_pandas_labels) == 1 and all(
            q[i] < q[i + 1] for i in range(len(q) - 1)
        ):
            # Use helper method without UNION ALL operations if the query compiler has only a single column
            # and the list of quantiles is sorted. _quantiles_single_col internally uses an UNPIVOT
            # where we cannot preserve order without adding an extra JOIN.
            #
            # The dtype of the resulting index column should always be float unless explicitly specified,
            # such as with `df.describe`, where the column should be strings.
            return query_compiler._quantiles_single_col(
                q, interpolation, index=index_values, index_dtype=index_dtype
            )
        original_frame = query_compiler._modin_frame
        data_column_pandas_labels = original_frame.data_column_pandas_labels
        if len(q) == 0:
            # Return empty frame; each column should be float as if it held percentiles
            return SnowflakeQueryCompiler.from_pandas(
                native_pd.DataFrame(
                    [],
                    columns=data_column_pandas_labels,
                    dtype=[np.float64] * len(data_column_pandas_labels),
                )
            )
        index_column_snowflake_quoted_identifier = (
            original_frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
                pandas_labels=[INDEX_LABEL],
                wrap_double_underscore=True,
            )[0]
        )
        global_ordering_identifier = (
            original_frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
                pandas_labels=[concat_utils.CONCAT_POSITION_COLUMN_LABEL],
            )[0]
        )
        # For each quantile and an N-column dataframe, create a 1x(N+2) frame with a column
        # for that quantile of the original column, one column with the quantile to use as the
        # index later, and one column for global ordering. Each frame is union_all'd together.
        ordered_dataframe = functools.reduce(
            lambda ordered_dataframe, new_col_frame: ordered_dataframe.union_all(
                new_col_frame
            ),
            itertools.starmap(
                lambda i, quantile: append_columns(
                    # Compute quantiles for each column
                    self._modin_frame.ordered_dataframe.agg(
                        *[
                            column_quantile(col(ident), interpolation, quantile).as_(
                                ident
                            )
                            for ident in original_frame.data_column_snowflake_quoted_identifiers
                        ]
                    ),
                    # Append a new column with the appropriate index label,
                    # and a global ordering column, since the result would otherwise sort rows by index
                    [
                        index_column_snowflake_quoted_identifier,
                        global_ordering_identifier,
                    ],
                    [pandas_lit(index_values[i]), pandas_lit(i)],
                ),
                enumerate(q),
            ),
        )
        # frontend ensured the result has at least one column
        assert (
            ordered_dataframe is not None
        ), "frame must have at least one column call to quantile"
        ordered_dataframe = ordered_dataframe.sort(
            OrderingColumn(global_ordering_identifier),
            *ordered_dataframe.ordering_columns,
        )
        return SnowflakeQueryCompiler(
            InternalFrame.create(
                ordered_dataframe=ordered_dataframe,
                data_column_pandas_labels=original_frame.data_column_pandas_labels,
                data_column_pandas_index_names=[None],
                data_column_snowflake_quoted_identifiers=original_frame.data_column_snowflake_quoted_identifiers,
                index_column_pandas_labels=[None],
                index_column_snowflake_quoted_identifiers=[
                    index_column_snowflake_quoted_identifier
                ],
                data_column_types=original_frame.cached_data_column_snowpark_pandas_types,
                index_column_types=[None],
            )
        )

    def _quantiles_single_col(
        self,
        q: list[float],
        interpolation: Literal["linear", "lower", "higher", "midpoint", "nearest"],
        index: Optional[Union[list[str], list[float]]] = None,
        index_dtype: npt.DTypeLike = float,
    ) -> "SnowflakeQueryCompiler":
        """
        Helper method for ``qcut`` and ``quantile`` to compute quantiles over frames with a single column.
        ``q`` must be sorted in ascending order (see Notes section).

        Normally, we compute single row for every given quantile, with each column corresponding to
        a column to the input frame.
        These rows are all UNION ALL'd together at the end in order to avoid costly JOIN or
        transpose (PIVOT/UNPIVOT) operations, as in the below diagram.

        pd.DataFrame({"a": [0, 1], "b": [1, 2]}).quantile([0.25, 0.75]):
        +-------+------+------+
        | index |    a |    b |
        +-------+------+------+                    +-------+------+------+
        |  0.25 | 0.25 | 1.25 |                    | index |    a |    b |
        +-------+------+------+                    +-------+------+------+
                                 -- UNION ALL -->  |  0.25 | 0.25 | 1.25 |
        +-------+------+------+                    +-------+------+------+
        | index |    a |    b |                    |  0.75 | 0.75 | 1.75 |
        +-------+------+------+                    +-------+------+------+
        |  0.75 | 0.75 | 1.75 |
        +-------+------+------+

        When the list ``q`` has many elements (as is the case for most uses of qcut), the number of
        UNION operations increases dramatically, and may cause Snowpark to create temporary tables.
        This greatly increases latency.

        When the input frame has a single column, we can eliminate UNION ALL operations
        by producing a single row where the columns are the different quantiles. Since there is
        only a single row, we can do a relatively cheap UNPIVOT to make the result a single column.

        pd.Series([0, 1], name="b").quantile([0.25, 0.75]):
        +------+------+                                              +-------+------+
        |   q1 |   q2 |                                              | index |    b |
        +------+------+  -- UNPIVOT(b FOR quantile IN (q1, q2)) -->  +-------+------+
        | 1.25 | 1.75 |                                              |    q1 | 1.25 |
        +------+------+                                              +-------+------+
                                                                     |    q2 | 1.75 |
                                                                     +-------+------+

        ``qcut`` can drop the index column afterwards, but ``quantile`` and ``describe`` keep it.

        Parameters
        ----------
        q : list[float]
            A list of floats representing the quantiles to compute, sorted in ascending order.
            In ``qcut`` and ``describe``, ``q`` is guaranteed to be sorted in the output.
            In ``quantile``, this is not guaranteed, and must be verified by the caller.
        interpolation : {"linear", "lower", "higher", "midpoint", "nearest"}
            See documentation for ``quantile``.
        index : list[str] | list[float], optional
            The labels for the resulting index column, allowing us to avoid a JOIN query by directly
            setting the correct column names before UNPIVOT. This is used primarily for ``describe``,
            where the resulting row labels are percentiles like "25%" rather than decimals like "0.25".
        index_dtype : npt.DtypeLike, default: float
            The type to which to coerce the resulting index column. Since UNPIVOT requires string column
            names, the resulting index column must be explicitly casted after the operation.

        Returns
        -------
        SnowflakeQueryCompiler
            A 1-column SnowflakeQueryCompiler with `index` as its index and the computed
            quantiles as its data column.

        Notes
        -----
        ``q`` must be sorted in ascending order, as OrderedFrame.unpivot will use the value column
        (``b`` in the above example table) as its ordering column. Although the underlying Snowpark
        DataFrame.unpivot operation nominally preserves the order of columns_list in the rows of
        the resulting output, we cannot use the ROW_POSITION operator without first providing an
        existing ordering column. Using transpose_single_row, or using a dummy index as the ordering
        column would allow us to create an accurate row position column, but would require a
        potentially expensive JOIN operator afterwards to apply the correct index labels.
        """
        assert len(self._modin_frame.data_column_pandas_labels) == 1

        if index is not None:
            # Snowpark UNPIVOT requires these to be strings
            index = list(map(str, index))
        original_frame = self._modin_frame
        col_label = original_frame.data_column_pandas_labels[0]
        col_identifier = original_frame.data_column_snowflake_quoted_identifiers[0]
        new_labels = [str(quantile) for quantile in q]
        new_identifiers = (
            original_frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
                pandas_labels=new_labels
            )
        )
        ordered_dataframe = original_frame.ordered_dataframe.agg(
            *[
                to_variant(
                    column_quantile(col(col_identifier), interpolation, quantile)
                ).as_(new_ident)
                for new_ident, quantile in zip(new_identifiers, q)
            ]
        )
        index_identifier = ordered_dataframe.generate_snowflake_quoted_identifiers(
            pandas_labels=[None]
        )[0]
        # In order to set index labels without a JOIN, we call unpivot directly instead of using
        # transpose_single_row. This also lets us avoid JSON serialization/deserialization.
        ordered_dataframe = ordered_dataframe.unpivot(
            col_identifier,
            index_identifier,
            new_identifiers,
            col_mapper=dict(zip(new_identifiers, index))
            if index is not None
            else dict(zip(new_identifiers, new_labels)),
        )
        col_after_cast_identifier = (
            ordered_dataframe.generate_snowflake_quoted_identifiers(
                pandas_labels=[col_label]
            )[0]
        )
        # Restore NULL values in the data column and cast back to float
        ordered_dataframe = ordered_dataframe.select(
            index_identifier,
            col(col_identifier).cast(FloatType()).as_(col_after_cast_identifier),
        ).ensure_row_position_column()
        internal_frame = InternalFrame.create(
            ordered_dataframe=ordered_dataframe,
            data_column_pandas_labels=[col_label],
            data_column_pandas_index_names=[None],
            data_column_snowflake_quoted_identifiers=[col_after_cast_identifier],
            index_column_pandas_labels=[None],
            index_column_snowflake_quoted_identifiers=[index_identifier],
            data_column_types=original_frame.cached_data_column_snowpark_pandas_types,
            index_column_types=None,
        )
        # We cannot call astype() directly to convert an index column, so we replicate
        # the logic here so we don't have to mess with set_index.
        internal_frame = (
            internal_frame.update_snowflake_quoted_identifiers_with_expressions(
                {
                    index_identifier: column_astype(
                        index_identifier,
                        TypeMapper.to_pandas(
                            internal_frame.get_snowflake_type(index_identifier)
                        ),
                        index_dtype,
                        TypeMapper.to_snowflake(index_dtype),
                    )
                }
            )[0]
        )

        return SnowflakeQueryCompiler(internal_frame)

    def skew(
        self,
        axis: int,
        skipna: bool,
        numeric_only: bool,
    ) -> "SnowflakeQueryCompiler":
        """
        Return unbiased skew, normalized over n-1

        Parameters
        ----------
        axis: Optional[int]
            Axis to calculate skew on, only 0 (columnar) is supported
        skipna: Optional[bool]
            Exclude NA values when calculating result ( only True is supported )
        numeric_only: Optional[bool]
            Include only the numeric columns ( only True is supported )
        level: Optional[bool]
            Not Supported, included for compatibility with other stats calls

        Returns
        -------
        SnowflakeQueryCompiler
            A query compiler containing skew for the numeric columns.
        """
        if axis == 1:
            raise ErrorMessage.not_implemented("axis = 1 not supported for skew")

        if numeric_only is not True:
            raise ErrorMessage.not_implemented(
                "numeric_only = False argument not supported for skew"
            )

        result = self.agg(
            func="skew",
            axis=0 if axis is None else axis,
            args={},
            kwargs={"numeric_only": numeric_only, "level": None, "skipna": True},
        )
        return result

    def describe(
        self,
        percentiles: np.ndarray,
    ) -> "SnowflakeQueryCompiler":
        """
        Summarizes statistics for the SnowflakeQueryCompiler.

        Parameters
        ----------
        percentiles: np.ndarray
            A list of percentiles to include in the output. Normalized by the frontend to be between 0 and 1.

        Returns
        -------
        SnowflakeQueryCompiler
            A query compiler containing descriptive statistics for this query compiler object.
        """
        # Per pandas docs, a described frame/series will have the following rows:
        # >>> df = pd.DataFrame({'categorical': pd.Categorical(['d','e','f']),
        # ...                    'numeric': [1, 2, 3],
        # ...                    'object': ['a', 'b', 'c']
        # ...                   })
        # >>> df.describe(include='all')
        #        categorical  numeric object
        # count            3      3.0      3
        # unique           3      NaN      3
        # top              f      NaN      a
        # freq             1      NaN      1
        # mean           NaN      2.0    NaN
        # std            NaN      1.0    NaN
        # min            NaN      1.0    NaN
        # 25%            NaN      1.5    NaN
        # 50%            NaN      2.0    NaN
        # 75%            NaN      2.5    NaN
        # max            NaN      3.0    NaN
        sorted_percentiles = sorted(percentiles)
        dtypes = self.dtypes
        # If we operate on the original frame's labels, then if two columns have the same name but
        # different one is `object` and one is numeric,, the JOIN behavior of SnowflakeQueryCompiler.concat
        # will produce incorrect results. For example, consider the following dataframe, where an
        # `object` column and `int64` column both share the label "a":
        #     +---+-----+---+-----+
        #     | a |  a  | b |  c  |
        #     +---+-----+---+-----+
        #     | 1 | 'x' | 3 | 'i' |
        #     +---+-----+---+-----+
        #     | 2 | 'y' | 4 | 'j' |
        #     +---+-----+---+-----+
        #     | 3 | 'x' | 5 | 'j' |
        #     +---+-----+---+-----+
        # For all `object` columns in the frame, we will generate a query compiler with the computed
        # `top`/`freq` statistics. Similarly, for the numeric columns we will generate a query compiler
        # containing the `std`, `min`/`max`, and other numeric statistics:
        #     OBJECT QUERY COMPILER    NUMERIC QUERY COMPILER
        #     +------+-----+-----+     +-----+-----+-----+
        #     |      |  a  |  c  |     |     |  a  |  b  |
        #     +------+-----+-----+     +-----+-----+-----+
        #     |  top | 'x' | 'j' |     | min |  1  |  3  |
        #     +------+-----+-----+     +-----+-----+-----+ (additional aggregations omitted)
        #     | freq |  2  |  2  |     | max |  3  |  5  |
        #     +------+-----+-----+     +-----+-----+-----+
        # We `concat` these two query compilers (+ an additional one for the `count` statistic computed
        # for all columns). Numeric columns will have NULL values for the `top` and `freq` statistics,
        # and object columns will have NULL values for `min`, `max`, etc. This is accomplished by
        # the `join="outer"` parameter, but it will still erroneously try to combine the aggregations
        # of the object and numeric columns that share a label.
        # To circumvent this, we relabel all columns with a simple integer index, and restore the
        # correct labels at the very end after `concat`.
        # The end result (before restoring the original pandas labels) should look something like this
        # (many rows omitted for brevity):
        #     Column mapping: {0: "a", 1: "a", 2: "b", 3: "c"}
        #     +------+-----+-----+              +-----+-----+-----+
        #     |      |  1  |  3  |              |     |  0  |  2  |
        #     +------+-----+-----+              +-----+-----+-----+
        #     |  top | 'x' | 'j' | -- CONCAT -- | min |  1  |  3  |
        #     +------+-----+-----+              +-----+-----+-----+
        #     | freq |  2  |  2  |              | max |  3  |  5  |
        #     +------+-----+-----+              +-----+-----+-----+
        #                               =
        #              +------+-----+------+-----+------+
        #              |      |  0  |   1  |  2  |   3  |
        #              +------+-----+------+-----+------+
        #              |  top | NaN |  'x' | NaN |  'j' |
        #              +------+-----+------+-----+------+
        #              | freq | NaN |   2  | NaN |   2  |
        #              +------+-----+------+-----+------+
        #              |  min |  1  | None |  3  | None |
        #              +------+-----+------+-----+------+
        #              |  max |  3  | None |  5  | None |
        #              +------+-----+------+-----+------+
        original_columns = self.columns
        query_compiler = self.set_columns(list(range(len(self.columns))))
        internal_frame = query_compiler._modin_frame
        # Compute count for all columns regardless of dtype
        query_compilers_to_concat = [
            query_compiler.agg(["count"], axis=0, args=[], kwargs={})
        ]
        # Separate object, numeric, and datetime columns to compute different statistics.
        # Datetime columns are treated as numeric, and have all statistics computed EXCEPT std.
        # If datetime columns appear in the same frame as other numeric ones, the `std` row appears
        # as the last row in the describe frame instead of its usual position.
        obj_column_pos = []
        numeric_column_pos = []
        datetime_column_pos = []
        for i, col_dtype in enumerate(dtypes.values):
            if is_datetime64_any_dtype(col_dtype):
                datetime_column_pos.append(i)
            elif is_numeric_dtype(col_dtype):
                numeric_column_pos.append(i)
            else:
                obj_column_pos.append(i)
        if len(obj_column_pos) > 0:
            obj_internal_frame = get_frame_by_col_pos(internal_frame, obj_column_pos)
            obj_qc = SnowflakeQueryCompiler(obj_internal_frame)
            unique_qc = obj_qc._nunique_columns(dropna=True)
            # If the index is empty, later GROUP BY calls would return with no rows because
            # there are no groups to group by. As such, we append a dummy row of NULL values to
            # avoid incurring an extra query from an explicit emptiness check; the later GROUP BY
            # to compute `freq` will ignore NULL values, so this will not affect the output.
            obj_col_labels = obj_qc._modin_frame.data_column_pandas_labels
            padded_qc = obj_qc.concat(
                other=[
                    SnowflakeQueryCompiler.from_pandas(
                        native_pd.DataFrame(
                            # Use a list comprehension instead of dict in case of duplicate labels
                            [[None] * len(obj_col_labels)],
                            columns=obj_col_labels,
                            dtype="O",
                        )
                    )
                ],
                join="inner",
                ignore_index=True,
                axis=0,
            )
            # Compute top (the mode of each column) + freq (the number of times this mode appears).
            # Also create a new identifier to track min(__row_position__) of each group to ensure stability
            (
                top_identifier,
                freq_identifier,
                min_row_position_identifier,
            ) = padded_qc._modin_frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
                pandas_labels=["top", "freq", "min_row_position"]
            )
            top_freq_identifiers = (top_identifier, freq_identifier)
            row_position_identifier = (
                padded_qc._modin_frame.ordered_dataframe.row_position_snowflake_quoted_identifier
            )
            # To accommodate multi-level columns in the source frame, we generate a new index column
            # in the top/freq frame for each level. We transpose this frame later, so the columns
            # of the transposed result will appropriately match those in the source frame.
            new_index_labels = [None] * padded_qc._modin_frame.num_index_levels(axis=1)
            new_index_identifiers = padded_qc._modin_frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
                pandas_labels=fill_none_in_index_labels(
                    index_labels=new_index_labels,
                    existing_labels=padded_qc._modin_frame.index_column_pandas_labels,
                )
            )

            def count_freqs(
                col_labels: Union[str, tuple[str, ...]], col_identifier: str
            ) -> OrderedDataFrame:
                """
                Helper function to compute the mode ("top") and frequency with which the mode
                appears ("count") for a given column.

                This helper returns a 1-row OrderedDataFrame with the columns "__index__", "top" and "freq",
                containing the column name, the mode of this column, and the number of times the mode
                occurs. This result should be UNION ALL'd together with the results from the other
                columns of the original frame, then transposed so "top" and "freq" are rows.
                If the source frame had multi-level columns, then "__level_0__", "__level_1__", etc.
                are returned instead of "__index__".

                This function performs a similar purpose to the existing QC.value_counts method, but
                we cannot use that or QC.groupby_agg because of differing behaviors with columns of
                with only NULL values. In the result of df.describe(), if all elements in a column
                are NULL, its reported `top` and `freq` should be NULL and NaN, respectively.
                QC.value_counts(dropna=True) ignores NULL values and would return an empty frame if
                the column only has NULLs.
                QC.value_counts(dropna=False) would correctly report NULL as the `top` item, but
                reports `freq` as the number of times NULL appears, which we do not want.
                """
                col_labels_tuple = (
                    col_labels if is_list_like(col_labels) else (col_labels,)
                )
                assert len(col_labels_tuple) == len(
                    new_index_identifiers
                ), f"level of labels {col_labels_tuple} did not match level of identifiers {new_index_identifiers}"
                # The below OrderedDataFrame operations are analogous to the following SQL for column "a":
                # SELECT 'a' AS __index__,
                #        a::VARIANT AS top,
                #        IFF(a IS NULL, NULL, COUNT(a)) AS freq
                # FROM df
                # GROUP BY a
                # ORDER BY freq DESC, MIN(__row_position__) ASC NULLS LAST
                # LIMIT 1
                #
                # The resulting 1-row frame for column "a": [1, 1, 2] will have the form
                # +-----------+-----+------+
                # | __index__ | top | freq |
                # +-----------+-----+------+
                # |         a |   1 |    2 |
                # +-----------+-----+------+
                #
                # which transposes to
                # +------+---+
                # |      | a |
                # +------+---+
                # |  top | 1 |
                # +------+---+
                # | freq | 2 |
                # +------+---+
                #
                # If the source frame had multi-level columns, the same logic holds, but we will have more
                # than one index column in the result. For example, the following 1-row frame is produced
                # for multi-level column ("a", "b"): [1, 1, 2].
                #
                # +-------------+-------------+-----+------+
                # | __level_0__ | __level_1__ | top | freq |
                # +-------------+-------------+-----+------+
                # |           a |           b |   1 |    2 |
                # +-------------+-------------+-----+------+
                #
                # This transposes to
                # +------+---+
                # |      | a |
                # +------+---+
                # |      | b |
                # +------+---+
                # |  top | 1 |
                # +------+---+
                # | freq | 2 |
                # +------+---+
                return (
                    padded_qc._modin_frame.ordered_dataframe.group_by(
                        [col_identifier],
                        [
                            iff(
                                col(col_identifier).is_null(),
                                pandas_lit(None),
                                count(col(col_identifier)),
                            ).as_(freq_identifier),
                            min_(row_position_identifier).as_(
                                min_row_position_identifier
                            ),
                        ],
                    )
                    .sort(
                        OrderingColumn(freq_identifier, ascending=False, na_last=True),
                        OrderingColumn(min_row_position_identifier, ascending=True),
                    )
                    .limit(1)
                    .select(
                        *(
                            # If the original frame had multi-level columns, we must create
                            # a multi-level index to transpose this frame later.
                            [
                                pandas_lit(col_label).as_(index_identifier)
                                for col_label, index_identifier in zip(
                                    col_labels_tuple, new_index_identifiers
                                )
                            ]
                            + [
                                col(col_identifier)
                                .cast(VariantType())
                                .as_(top_identifier),
                                freq_identifier,
                            ]
                        )
                    )
                )

            # count_freqs produces a 1-row frame with the column label(s), top element, and frequency
            # for each column in the source frame; we union these all together and transpose the
            # result to match the output of describe().
            ordered_dataframe = functools.reduce(
                lambda concat_frame, new_ordered_frame: concat_frame.union_all(
                    new_ordered_frame
                ),
                itertools.starmap(
                    count_freqs,
                    zip(
                        obj_col_labels,
                        padded_qc._modin_frame.data_column_snowflake_quoted_identifiers,
                    ),
                ),
            ).ensure_row_position_column()
            top_freq_qc = SnowflakeQueryCompiler(
                InternalFrame.create(
                    ordered_dataframe=ordered_dataframe,
                    data_column_pandas_labels=["top", "freq"],
                    data_column_pandas_index_names=[None],
                    data_column_snowflake_quoted_identifiers=top_freq_identifiers,
                    index_column_pandas_labels=new_index_labels,
                    index_column_snowflake_quoted_identifiers=new_index_identifiers,
                    data_column_types=[None, None],
                    index_column_types=[None] * len(new_index_labels),
                )
            ).transpose()
            query_compilers_to_concat.extend([unique_qc, top_freq_qc])

        # It's easier to perform multiple .agg calls and concat them than it is to perform a
        # single call and reorder everything.
        # Every aggregation in a list generates a new SELECT anyway, so it doesn't
        # substantially impact query text size.
        if len(datetime_column_pos) > 0:

            def get_qcs_for_numeric_and_datetime_cols(
                numeric_and_datetime_frame: InternalFrame,
            ) -> list[SnowflakeQueryCompiler]:
                """
                Helper function to compute aggregation statistics on datetime columns by casting
                them to NS since epoch, performing the computation, and casting them back.

                Returns the list of query compilers for the performed aggregations, after converting
                back to the appropriate datetime type.
                """
                # Can't use QC.astype() in case of duplicate columns since that requires label keys
                numeric_and_datetime_frame_types = numeric_and_datetime_frame.get_snowflake_type(
                    numeric_and_datetime_frame.data_column_snowflake_quoted_identifiers
                )
                # Convert datetime cols to NS since epoch
                datetime_as_epoch_qc = SnowflakeQueryCompiler(
                    numeric_and_datetime_frame.update_snowflake_quoted_identifiers_with_expressions(
                        {
                            ident: column_astype(
                                ident,
                                from_sf_type=sf_type,
                                to_dtype=np.int64,
                                to_sf_type=TypeMapper.to_snowflake(np.int64),
                            )
                            for ident, sf_type in zip(
                                numeric_and_datetime_frame.data_column_snowflake_quoted_identifiers,
                                numeric_and_datetime_frame_types,
                            )
                            if isinstance(sf_type, TimestampType)
                        }
                    ).frame
                )
                # Convert aggregation results from NS since epoch back to datetimes
                return [
                    SnowflakeQueryCompiler(
                        agg_qc._modin_frame.update_snowflake_quoted_identifiers_with_expressions(
                            {
                                ident: column_astype(
                                    ident,
                                    from_sf_type=TypeMapper.to_snowflake(np.int64),
                                    to_dtype=TypeMapper.to_pandas(original_sf_type),
                                    to_sf_type=original_sf_type,
                                )
                                for ident, original_sf_type in zip(
                                    agg_qc._modin_frame.data_column_snowflake_quoted_identifiers,
                                    numeric_and_datetime_frame_types,
                                )
                                if isinstance(original_sf_type, TimestampType)
                            }
                        ).frame
                    )
                    for agg_qc in [
                        datetime_as_epoch_qc.agg(
                            ["mean", "min"],
                            axis=0,
                            args=[],
                            kwargs={},
                        ),
                        datetime_as_epoch_qc.quantiles_along_axis0(
                            sorted_percentiles,
                            numeric_only=True,
                            index=format_percentiles(sorted_percentiles),
                            index_dtype=str,
                        ),
                        datetime_as_epoch_qc.agg(
                            ["max"],
                            axis=0,
                            args=[],
                            kwargs={},
                        ),
                    ]
                ]

            numeric_and_datetime_frame = get_frame_by_col_pos(
                internal_frame, sorted(numeric_column_pos + datetime_column_pos)
            )
            query_compilers_to_concat.extend(
                get_qcs_for_numeric_and_datetime_cols(numeric_and_datetime_frame)
            )
            # If datetime and numeric columns both exist, then place std at the bottom
            # and only compute std for numeric columns (not datetime)
            # If datetime columns exist and numeric columns don't, skip the std aggregation
            if len(numeric_column_pos) > 0:
                numeric_qc = SnowflakeQueryCompiler(
                    get_frame_by_col_pos(internal_frame, numeric_column_pos)
                )
                query_compilers_to_concat.append(
                    numeric_qc.agg(["std"], axis=0, args=[], kwargs={})
                )
        elif len(numeric_column_pos) > 0:
            # If numeric columns exist and datetime columns don't, place std between mean and min
            numeric_qc = SnowflakeQueryCompiler(
                get_frame_by_col_pos(internal_frame, numeric_column_pos)
            )
            query_compilers_to_concat.extend(
                [
                    numeric_qc.agg(
                        ["mean", "std", "min"],
                        axis=0,
                        args=[],
                        kwargs={},
                    ),
                    numeric_qc.quantiles_along_axis0(
                        sorted_percentiles,
                        numeric_only=True,
                        index=format_percentiles(sorted_percentiles),
                        index_dtype=str,
                    ),
                    numeric_qc.agg(
                        ["max"],
                        axis=0,
                        args=[],
                        kwargs={},
                    ),
                ]
            )
        # There must be more than one QC at this point -- all columns have one for count, obj columns
        # will have unique + top/freq, and numeric will have mean/quantiles/max. If there is only
        # one QC, then columns in the QC were somehow neither numeric nor non-numeric, which
        # is not possible (dfs with no columns were already handled by the frontend).
        assert (
            len(query_compilers_to_concat) > 1
        ), "must have more than one QC to concat"
        return (
            query_compilers_to_concat[0].concat(
                other=query_compilers_to_concat[1:],
                axis=0,
                join="outer",
            )
            # Restore the original pandas labels
            .set_columns(original_columns)
        )

    def sample(
        self,
        n: Optional[int],
        frac: Optional[float],
        replace: bool,
        weights: Optional[Union[str, np.ndarray]] = None,
        random_state: Optional[RandomState] = None,
        axis: Optional[int] = 0,
        ignore_index: Optional[bool] = False,
    ) -> "SnowflakeQueryCompiler":
        """
        The implementation to sample rows on a dataframe

        Args:
            n: Number of rows to return. Cannot be used with `frac`.
            frac: Fraction of rows to return. Cannot be used with `n`.
            replace : bool, default False
                Allow or disallow sampling of the same row more than once.
            weights : str or ndarray-like, optional
                Default 'None' results in equal probability weighting.
                If passed a Series, will align with target object on index. Index
                values in weights not found in sampled object will be ignored and
                index values in sampled object not in weights will be assigned
                weights of zero.
                If called on a DataFrame, will accept the name of a column
                when axis = 0.
                Unless weights are a Series, weights must be same length as axis
                being sampled.
                If weights do not sum to 1, they will be normalized to sum to 1.
                Missing values in the weights column will be treated as zero.
                Infinite values not allowed.
            random_state : int, array-like, BitGenerator, np.random.RandomState, np.random.Generator, optional
                If int, array-like, or BitGenerator, seed for random number generator.
                If np.random.RandomState or np.random.Generator, use as given.
            axis : {0, 1}, default None
                Axis to sample. Accepts axis number or name. Default is stat axis
                for given data type. For `Series` this parameter is unused and defaults to `None`.
            ignore_index : bool, default False
                If True, the resulting index will be labeled 0, 1, …, n - 1.

        Returns:
            The sampled query compiler
        """
        if axis == 1:
            # i.e., axis = 1, use native pandas sample method to get the column sample positions
            pandas_sample = pandas.DataFrame(columns=range(len(self.columns))).sample(
                n=n,
                frac=frac,
                replace=replace,
                weights=weights,
                random_state=random_state,
                axis=axis,
                ignore_index=ignore_index,
            )

            # use the sample column positions to create the sample dataframe
            return self.take_2d_positional(
                index=slice(None), columns=pandas_sample.columns
            )

        # handle axis = 0
        if weights is not None:
            ErrorMessage.not_implemented("`weights` is not supported.")

        if random_state is not None:
            ErrorMessage.not_implemented("`random_state` is not supported.")

        assert n is not None or frac is not None
        frame = self._modin_frame
        if replace:
            snowflake_quoted_identifiers = generate_snowflake_quoted_identifiers_helper(
                pandas_labels=[
                    ROW_POSITION_COLUMN_LABEL,
                    SAMPLED_ROW_POSITION_COLUMN_LABEL,
                ]
            )

            pre_sampling_rowcount = self.get_axis_len(axis=0)
            if n is not None:
                post_sampling_rowcount = n
            else:
                assert frac is not None
                post_sampling_rowcount = round(frac * pre_sampling_rowcount)

            row_position_col = (
                row_number()
                .over(Window.order_by(pandas_lit(1)))
                .as_(snowflake_quoted_identifiers[0])
            )

            sampled_row_position_col = uniform(
                0, pre_sampling_rowcount - 1, random()
            ).as_(snowflake_quoted_identifiers[1])

            sampled_row_positions_snowpark_frame = pd.session.generator(
                row_position_col,
                sampled_row_position_col,
                rowcount=post_sampling_rowcount,
            )

            sampled_row_positions_odf = OrderedDataFrame(
                dataframe_ref=DataFrameReference(sampled_row_positions_snowpark_frame),
                projected_column_snowflake_quoted_identifiers=snowflake_quoted_identifiers,
            )
            sampled_odf = cache_result(
                sampled_row_positions_odf.join(
                    right=self._modin_frame.ordered_dataframe,
                    left_on_cols=[snowflake_quoted_identifiers[1]],
                    right_on_cols=[
                        self._modin_frame.ordered_dataframe.row_position_snowflake_quoted_identifier
                    ],
                )
            )
        else:
            sampled_odf = frame.ordered_dataframe.sample(n=n, frac=frac)
        logging.warning(
            "Snowpark pandas `sample` will create a temp table for sampled results to keep it deterministic."
        )
        res = SnowflakeQueryCompiler(
            InternalFrame.create(
                ordered_dataframe=sampled_odf,
                data_column_pandas_labels=frame.data_column_pandas_labels,
                data_column_pandas_index_names=frame.data_column_pandas_index_names,
                data_column_snowflake_quoted_identifiers=frame.data_column_snowflake_quoted_identifiers,
                index_column_pandas_labels=frame.index_column_pandas_labels,
                index_column_snowflake_quoted_identifiers=frame.index_column_snowflake_quoted_identifiers,
                data_column_types=frame.cached_data_column_snowpark_pandas_types,
                index_column_types=frame.cached_index_column_snowpark_pandas_types,
            )
        )
        if ignore_index:
            res = res.reset_index(drop=True)
        return res

    # Window API

    def window_mean(
        self,
        fold_axis: Union[int, str],
        window_kwargs: dict,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        ErrorMessage.method_not_implemented_error(
            name="mean", class_="Window"
        )  # pragma: no cover

    def window_sum(
        self,
        fold_axis: Union[int, str],
        window_kwargs: dict,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        ErrorMessage.method_not_implemented_error(
            name="sum", class_="Window"
        )  # pragma: no cover

    def window_var(
        self,
        fold_axis: Union[int, str],
        window_kwargs: dict,
        ddof: int = 1,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        ErrorMessage.method_not_implemented_error(
            name="var", class_="Window"
        )  # pragma: no cover

    def window_std(
        self,
        fold_axis: Union[int, str],
        window_kwargs: dict,
        ddof: int = 1,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        ErrorMessage.method_not_implemented_error(
            name="std", class_="Window"
        )  # pragma: no cover

    # Rolling API

    def rolling_count(
        self,
        fold_axis: Union[int, str],
        rolling_kwargs: dict,
        numeric_only: bool = False,
        *args: Any,
        **kwargs: Any,
    ) -> "SnowflakeQueryCompiler":
        return self._window_agg(
            window_func=WindowFunction.ROLLING,
            agg_func="count",
            window_kwargs=rolling_kwargs,
            agg_kwargs=dict(numeric_only=numeric_only),
        )

    def rolling_sum(
        self,
        fold_axis: Union[int, str],
        rolling_kwargs: dict,
        numeric_only: bool = False,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> "SnowflakeQueryCompiler":
        WarningMessage.warning_if_engine_args_is_set(
            "rolling_sum", engine, engine_kwargs
        )
        return self._window_agg(
            window_func=WindowFunction.ROLLING,
            agg_func="sum",
            window_kwargs=rolling_kwargs,
            agg_kwargs=dict(numeric_only=numeric_only),
        )

    def rolling_mean(
        self,
        fold_axis: Union[int, str],
        rolling_kwargs: dict,
        numeric_only: bool = False,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> "SnowflakeQueryCompiler":
        WarningMessage.warning_if_engine_args_is_set(
            "rolling_mean", engine, engine_kwargs
        )
        return self._window_agg(
            window_func=WindowFunction.ROLLING,
            agg_func="mean",
            window_kwargs=rolling_kwargs,
            agg_kwargs=dict(numeric_only=numeric_only),
        )

    def rolling_median(
        self,
        fold_axis: Union[int, str],
        rolling_kwargs: dict,
        numeric_only: bool = False,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
        **kwargs: Any,
    ) -> None:
        ErrorMessage.method_not_implemented_error(name="median", class_="Rolling")

    def rolling_var(
        self,
        fold_axis: Union[int, str],
        rolling_kwargs: dict,
        ddof: int = 1,
        numeric_only: bool = False,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> "SnowflakeQueryCompiler":
        WarningMessage.warning_if_engine_args_is_set(
            "rolling_var", engine, engine_kwargs
        )
        return self._window_agg(
            window_func=WindowFunction.ROLLING,
            agg_func="var",
            window_kwargs=rolling_kwargs,
            agg_kwargs=dict(ddof=ddof, numeric_only=numeric_only),
        )

    def rolling_std(
        self,
        fold_axis: Union[int, str],
        rolling_kwargs: dict,
        ddof: int = 1,
        numeric_only: bool = False,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> "SnowflakeQueryCompiler":
        WarningMessage.warning_if_engine_args_is_set(
            "rolling_var", engine, engine_kwargs
        )
        return self._window_agg(
            window_func=WindowFunction.ROLLING,
            agg_func="std",
            window_kwargs=rolling_kwargs,
            agg_kwargs=dict(ddof=ddof, numeric_only=numeric_only),
        )

    def rolling_min(
        self,
        fold_axis: Union[int, str],
        rolling_kwargs: dict,
        numeric_only: bool = False,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> "SnowflakeQueryCompiler":
        WarningMessage.warning_if_engine_args_is_set(
            "rolling_min", engine, engine_kwargs
        )
        return self._window_agg(
            window_func=WindowFunction.ROLLING,
            agg_func="min",
            window_kwargs=rolling_kwargs,
            agg_kwargs=dict(numeric_only=numeric_only),
        )

    def rolling_max(
        self,
        fold_axis: Union[int, str],
        rolling_kwargs: dict,
        numeric_only: bool = False,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> "SnowflakeQueryCompiler":
        WarningMessage.warning_if_engine_args_is_set(
            "rolling_max", engine, engine_kwargs
        )
        return self._window_agg(
            window_func=WindowFunction.ROLLING,
            agg_func="max",
            window_kwargs=rolling_kwargs,
            agg_kwargs=dict(numeric_only=numeric_only),
        )

    def rolling_corr(
        self,
        fold_axis: Union[int, str],
        rolling_kwargs: dict,
        other: Optional[SnowparkDataFrame] = None,
        pairwise: Optional[bool] = None,
        ddof: int = 1,
        numeric_only: bool = False,
        **kwargs: Any,
    ) -> "SnowflakeQueryCompiler":
        if other is None:
            ErrorMessage.parameter_not_implemented_error(
                parameter_name="other = None", method_name="Rolling.corr"
            )
        if pairwise:
            ErrorMessage.parameter_not_implemented_error(
                parameter_name="pairwise = True", method_name="Rolling.corr"
            )
        return self._window_agg(
            window_func=WindowFunction.ROLLING,
            agg_func="corr",
            window_kwargs=rolling_kwargs,
            agg_kwargs=dict(
                numeric_only=numeric_only, other=other, pairwise=pairwise, ddof=ddof
            ),
        )

    def rolling_cov(
        self,
        fold_axis: Union[int, str],
        rolling_kwargs: dict,
        other: Optional[SnowparkDataFrame] = None,
        pairwise: Optional[bool] = None,
        ddof: int = 1,
        numeric_only: bool = False,
        **kwargs: Any,
    ) -> None:
        ErrorMessage.method_not_implemented_error(name="cov", class_="Rolling")

    def rolling_skew(
        self,
        fold_axis: Union[int, str],
        rolling_kwargs: dict,
        numeric_only: bool = False,
        **kwargs: Any,
    ) -> None:
        ErrorMessage.method_not_implemented_error(name="skew", class_="Rolling")

    def rolling_kurt(
        self,
        fold_axis: Union[int, str],
        rolling_kwargs: dict,
        numeric_only: bool = False,
        **kwargs: Any,
    ) -> None:
        ErrorMessage.method_not_implemented_error(name="kurt", class_="Rolling")

    def rolling_apply(
        self,
        fold_axis: Union[int, str],
        rolling_kwargs: dict,
        func: Any,
        raw: bool = False,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
        args: Optional[tuple] = None,
        kwargs: Optional[dict] = None,
    ) -> None:
        ErrorMessage.method_not_implemented_error(name="apply", class_="Rolling")

    def rolling_aggregate(
        self,
        fold_axis: Union[int, str],
        rolling_kwargs: dict,
        func: Union[str, list, dict],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        ErrorMessage.method_not_implemented_error(name="aggregate", class_="Rolling")

    def rolling_quantile(
        self,
        fold_axis: Union[int, str],
        rolling_kwargs: dict,
        quantile: float,
        interpolation: str = "linear",
        numeric_only: bool = False,
        **kwargs: Any,
    ) -> None:
        ErrorMessage.method_not_implemented_error(name="quantile", class_="Rolling")

    def rolling_sem(
        self,
        fold_axis: Union[int, str],
        rolling_kwargs: dict,
        ddof: int = 1,
        numeric_only: bool = False,
        *args: Any,
        **kwargs: Any,
    ) -> "SnowflakeQueryCompiler":
        return self._window_agg(
            window_func=WindowFunction.ROLLING,
            agg_func="sem",
            window_kwargs=rolling_kwargs,
            agg_kwargs=dict(ddof=ddof, numeric_only=numeric_only),
        )

    def rolling_rank(
        self,
        fold_axis: Union[int, str],
        rolling_kwargs: dict,
        method: str = "average",
        ascending: bool = True,
        pct: bool = False,
        numeric_only: bool = False,
        **kwargs: Any,
    ) -> None:
        ErrorMessage.method_not_implemented_error(name="rank", class_="Rolling")

    def _window_agg(
        self,
        window_func: WindowFunction,
        agg_func: AggFuncType,
        window_kwargs: dict[str, Any],
        agg_kwargs: dict[str, Any],
    ) -> "SnowflakeQueryCompiler":
        """
        Compute rolling window with given aggregation.
        Args:
            window_func: the type of window function to apply.
            agg_func: callable, str, list or dict. the aggregation function used.
            rolling_kwargs: keyword arguments passed to rolling.
            agg_kwargs: keyword arguments passed for the aggregation function.
        Returns:
            SnowflakeQueryCompiler: with a newly constructed internal dataframe
        """

        window = window_kwargs.get("window")
        min_periods = window_kwargs.get("min_periods")
        center = window_kwargs.get("center")
        numeric_only = agg_kwargs.get("numeric_only", False)
        query_compiler = self
        if numeric_only:
            # Include only float, int, and boolean columns
            query_compiler = drop_non_numeric_data_columns(
                query_compiler=self, pandas_labels_for_columns_to_exclude=[]
            )

        # Throw NotImplementedError if any parameter is unsupported
        if window_func == WindowFunction.ROLLING:
            check_and_raise_error_rolling_window_supported_by_snowflake(window_kwargs)
        elif window_func == WindowFunction.EXPANDING:
            check_and_raise_error_expanding_window_supported_by_snowflake(window_kwargs)

        frame = query_compiler._modin_frame.ensure_row_position_column()
        row_position_quoted_identifier = frame.row_position_snowflake_quoted_identifier

        if isinstance(window, int) or window_func == WindowFunction.EXPANDING:
            if center:
                # -(window // 2) is equivalent to window // 2 PRECEDING
                rows_between_start = -(window // 2)  # type: ignore
                rows_between_end = (window - 1) // 2  # type: ignore
            else:
                if window_func == WindowFunction.ROLLING:
                    # 1 - window is equivalent to window - 1 PRECEDING
                    rows_between_start = 1 - window  # type: ignore
                else:
                    rows_between_start = Window.UNBOUNDED_PRECEDING
                rows_between_end = Window.CURRENT_ROW

            window_expr = Window.orderBy(
                col(row_position_quoted_identifier)
            ).rows_between(rows_between_start, rows_between_end)

            if window_func == WindowFunction.ROLLING:
                # min_periods defaults to the size of the window if window is specified by an integer
                min_periods = window if min_periods is None else min_periods
            else:
                assert window_func == WindowFunction.EXPANDING
                # Handle case where min_periods = None
                min_periods = 0 if min_periods is None else min_periods
        else:
            assert isinstance(window, str) and window_func == WindowFunction.ROLLING
            if center:
                ErrorMessage.not_implemented(
                    f"'center=True' is not implemented with str window for Rolling.{agg_func}"
                )
            # min_periods defaults to 1 if window is time-based string/offset
            min_periods = 1 if min_periods is None else min_periods
            if self.is_multiindex():
                raise ValueError(
                    "Rolling behavior is undefined when used with a MultiIndex"
                )
            index_quoted_identifier = frame.index_column_snowflake_quoted_identifiers[0]
            window_expr = Window.orderBy(index_quoted_identifier).range_between(
                -create_snowpark_interval_from_window(window), Window.CURRENT_ROW
            )

        input_contains_timedelta = any(
            isinstance(t, TimedeltaType)
            for t in frame.cached_data_column_snowpark_pandas_types
        )

        # Perform Aggregation over the window_expr
        if agg_func == "sem":
            if input_contains_timedelta:
                raise DataError(_TIMEDELTA_ROLLING_AGGREGATION_NOT_SUPPORTED)

            # Standard error of mean (SEM) does not have native Snowflake engine support
            # so calculate as STDDEV/SQRT(N-ddof)
            ddof = agg_kwargs.get("ddof", 1)
            new_frame = frame.update_snowflake_quoted_identifiers_with_expressions(
                {
                    quoted_identifier: iff(
                        count(col(quoted_identifier)).over(window_expr) >= min_periods,
                        when(
                            # If STDDEV is Null (like when the window has 1 element), return NaN
                            # Note that in Python, np.nan / np.inf results in np.nan, so this check must come first
                            builtin("stddev")(col(quoted_identifier))
                            .over(window_expr)
                            .is_null(),
                            pandas_lit(None),
                        )
                        .when(
                            # Elif (N-ddof) is negative number, return NaN to mimic pandas sqrt of a negative number
                            count(col(quoted_identifier)).over(window_expr) - ddof < 0,
                            pandas_lit(None),
                        )
                        .when(
                            # Elif (N-ddof) is 0, return np.inf to mimic pandas division by 0
                            count(col(quoted_identifier)).over(window_expr) - ddof == 0,
                            pandas_lit(np.inf),
                        )
                        .otherwise(
                            # Else compute STDDEV/SQRT(N-ddof)
                            builtin("stddev")(col(quoted_identifier)).over(window_expr)
                            / builtin("sqrt")(
                                count(col(quoted_identifier)).over(window_expr) - ddof
                            ),
                        ),
                        pandas_lit(None),
                    )
                    for quoted_identifier in frame.data_column_snowflake_quoted_identifiers
                }
            ).frame
        elif agg_func == "corr":
            if input_contains_timedelta:
                ErrorMessage.not_implemented(_TIMEDELTA_ROLLING_CORR_NOT_SUPPORTED)
            if not isinstance(window, int):
                ErrorMessage.not_implemented(
                    "Snowpark pandas does not yet support non-integer 'window' for 'Rolling.corr'"
                )
            if window != min_periods:
                ErrorMessage.not_implemented(
                    f"min_periods {min_periods} must be == window {window} for 'Rolling.corr'"
                )
            assert window == min_periods
            other = agg_kwargs.get("other", None)
            other_qc = other._query_compiler
            result_frame, result_column_mapper = join_utils.align(
                left=frame,
                right=other_qc._modin_frame,
                left_on=frame.index_column_snowflake_quoted_identifiers,
                right_on=other_qc._modin_frame.index_column_snowflake_quoted_identifiers,
            )

            # columns that exist in both dfs
            matching_col_label_dict = {}
            for i in range(len(frame.data_column_pandas_labels)):
                frame_label = frame.data_column_pandas_labels[i]
                frame_identifier = frame.data_column_snowflake_quoted_identifiers[i]
                other_frame_identifier = (
                    other_qc._modin_frame.data_column_snowflake_quoted_identifiers[i]
                )
                if frame_label in other_qc._modin_frame.data_column_pandas_labels:
                    matching_col_label_dict[frame_label] = [
                        frame_identifier,
                        other_frame_identifier,
                    ]

            corr_result = result_frame
            # columns unique to the left or right hand side dfs
            wanted_cols = []
            wanted_col_values = []

            for x in result_frame.data_column_pandas_labels:
                if x not in matching_col_label_dict:
                    wanted_cols.append(x)
                    wanted_col_values.append(pandas_lit(None))

            corr_result = corr_result.project_columns(wanted_cols, wanted_col_values)

            for matching_label in matching_col_label_dict:
                quoted_identifier = result_column_mapper.left_quoted_identifiers_map[
                    matching_col_label_dict[matching_label][0]
                ]
                other_quoted_identifier = (
                    result_column_mapper.right_quoted_identifiers_map[
                        matching_col_label_dict[matching_label][1]
                    ]
                )

                corr_column = get_rolling_corr_column(
                    quoted_identifier, other_quoted_identifier, window_expr, window
                )
                corr_result_frame = corr_result.append_column(
                    unquote_name_if_quoted(matching_label), corr_column
                )

            # final frame columns are sorted lexicographically from the 2 original frames
            ordered_columns = sorted(corr_result_frame.data_column_pandas_labels)
            new_qc = SnowflakeQueryCompiler(corr_result_frame)
            new_qc = new_qc.take_2d_labels(index=slice(None), columns=ordered_columns)
            new_frame = new_qc._modin_frame
        else:
            snowflake_agg_func = get_snowflake_agg_func(agg_func, agg_kwargs, axis=0)
            if snowflake_agg_func is None:
                # We don't have test coverage for this situation because we
                # test individual rolling and expanding methods we've implemented,
                # like rolling_sum(), but other rolling methods raise
                # NotImplementedError immediately. We also don't support rolling
                # agg(), which might take us here.
                ErrorMessage.not_implemented(  # pragma: no cover
                    f"Window aggregation does not support the aggregation {repr_aggregate_function(agg_func, agg_kwargs)}"
                )
            if (
                snowflake_agg_func.snowpark_aggregation is not count
                # pandas only supports rolling timedelta aggregation for
                # count(), so we do the same.
                and input_contains_timedelta
            ):
                raise DataError(_TIMEDELTA_ROLLING_AGGREGATION_NOT_SUPPORTED)

            new_frame = frame.update_snowflake_quoted_identifiers_with_expressions(
                {
                    # If aggregation is count use count on row_position_quoted_identifier
                    # to include NULL values for min_periods comparison
                    quoted_identifier: iff(
                        count(col(row_position_quoted_identifier)).over(window_expr)
                        >= min_periods
                        if agg_func == "count"
                        else count(col(quoted_identifier)).over(window_expr)
                        >= min_periods,
                        snowflake_agg_func.snowpark_aggregation(
                            # Expanding is cumulative so replace NULL with 0 for sum aggregation
                            builtin("zeroifnull")(col(quoted_identifier))
                            if window_func == WindowFunction.EXPANDING
                            and agg_func == "sum"
                            else col(quoted_identifier)
                        ).over(window_expr),
                        pandas_lit(None),
                    )
                    for quoted_identifier in frame.data_column_snowflake_quoted_identifiers
                }
            ).frame
        return self.__constructor__(new_frame)

    def expanding_count(
        self,
        fold_axis: Union[int, str],
        expanding_kwargs: dict,
        numeric_only: bool = False,
    ) -> "SnowflakeQueryCompiler":
        return self._window_agg(
            window_func=WindowFunction.EXPANDING,
            agg_func="count",
            window_kwargs=expanding_kwargs,
            agg_kwargs=dict(numeric_only=numeric_only),
        )

    def expanding_sum(
        self,
        fold_axis: Union[int, str],
        expanding_kwargs: dict,
        numeric_only: bool = False,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
    ) -> "SnowflakeQueryCompiler":
        WarningMessage.warning_if_engine_args_is_set(
            "expanding_sum", engine, engine_kwargs
        )
        return self._window_agg(
            window_func=WindowFunction.EXPANDING,
            agg_func="sum",
            window_kwargs=expanding_kwargs,
            agg_kwargs=dict(numeric_only=numeric_only),
        )

    def expanding_mean(
        self,
        fold_axis: Union[int, str],
        expanding_kwargs: dict,
        numeric_only: bool = False,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
    ) -> "SnowflakeQueryCompiler":
        WarningMessage.warning_if_engine_args_is_set(
            "expanding_mean", engine, engine_kwargs
        )
        return self._window_agg(
            window_func=WindowFunction.EXPANDING,
            agg_func="mean",
            window_kwargs=expanding_kwargs,
            agg_kwargs=dict(numeric_only=numeric_only),
        )

    def expanding_median(
        self,
        fold_axis: Union[int, str],
        expanding_kwargs: dict,
        numeric_only: bool = False,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
    ) -> None:
        ErrorMessage.method_not_implemented_error(name="median", class_="Expanding")

    def expanding_var(
        self,
        fold_axis: Union[int, str],
        expanding_kwargs: dict,
        ddof: int = 1,
        numeric_only: bool = False,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
    ) -> "SnowflakeQueryCompiler":
        WarningMessage.warning_if_engine_args_is_set(
            "rolling_var", engine, engine_kwargs
        )
        return self._window_agg(
            window_func=WindowFunction.EXPANDING,
            agg_func="var",
            window_kwargs=expanding_kwargs,
            agg_kwargs=dict(ddof=ddof, numeric_only=numeric_only),
        )

    def expanding_std(
        self,
        fold_axis: Union[int, str],
        expanding_kwargs: dict,
        ddof: int = 1,
        numeric_only: bool = False,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
    ) -> "SnowflakeQueryCompiler":
        WarningMessage.warning_if_engine_args_is_set(
            "rolling_std", engine, engine_kwargs
        )
        return self._window_agg(
            window_func=WindowFunction.EXPANDING,
            agg_func="std",
            window_kwargs=expanding_kwargs,
            agg_kwargs=dict(ddof=ddof, numeric_only=numeric_only),
        )

    def expanding_min(
        self,
        fold_axis: Union[int, str],
        expanding_kwargs: dict,
        numeric_only: bool = False,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
    ) -> "SnowflakeQueryCompiler":
        WarningMessage.warning_if_engine_args_is_set(
            "expanding_min", engine, engine_kwargs
        )
        return self._window_agg(
            window_func=WindowFunction.EXPANDING,
            agg_func="min",
            window_kwargs=expanding_kwargs,
            agg_kwargs=dict(numeric_only=numeric_only),
        )

    def expanding_max(
        self,
        fold_axis: Union[int, str],
        expanding_kwargs: dict,
        numeric_only: bool = False,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
    ) -> "SnowflakeQueryCompiler":
        WarningMessage.warning_if_engine_args_is_set(
            "expanding_max", engine, engine_kwargs
        )
        return self._window_agg(
            window_func=WindowFunction.EXPANDING,
            agg_func="max",
            window_kwargs=expanding_kwargs,
            agg_kwargs=dict(numeric_only=numeric_only),
        )

    def expanding_corr(
        self,
        fold_axis: Union[int, str],
        expanding_kwargs: dict,
        other: Optional[SnowparkDataFrame] = None,
        pairwise: Optional[bool] = None,
        ddof: int = 1,
        numeric_only: bool = False,
    ) -> None:
        ErrorMessage.method_not_implemented_error(name="corr", class_="Expanding")

    def expanding_cov(
        self,
        fold_axis: Union[int, str],
        expanding_kwargs: dict,
        other: Optional[SnowparkDataFrame] = None,
        pairwise: Optional[bool] = None,
        ddof: int = 1,
        numeric_only: bool = False,
    ) -> None:
        ErrorMessage.method_not_implemented_error(name="cov", class_="Expanding")

    def expanding_skew(
        self,
        fold_axis: Union[int, str],
        expanding_kwargs: dict,
        numeric_only: bool = False,
    ) -> None:
        ErrorMessage.method_not_implemented_error(name="skew", class_="Expanding")

    def expanding_kurt(
        self,
        fold_axis: Union[int, str],
        expanding_kwargs: dict,
        numeric_only: bool = False,
    ) -> None:
        ErrorMessage.method_not_implemented_error(name="kurt", class_="Expanding")

    def expanding_apply(
        self,
        fold_axis: Union[int, str],
        expanding_kwargs: dict,
        func: Any,
        raw: bool = False,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
        args: Optional[tuple] = None,
        kwargs: Optional[dict] = None,
    ) -> None:
        ErrorMessage.method_not_implemented_error(name="apply", class_="Expanding")

    def expanding_aggregate(
        self,
        fold_axis: Union[int, str],
        expanding_kwargs: dict,
        func: Any,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        ErrorMessage.method_not_implemented_error(name="aggregate", class_="Expanding")

    def expanding_quantile(
        self,
        fold_axis: Union[int, str],
        expanding_kwargs: dict,
        quantile: float,
        interpolation: str = "linear",
        numeric_only: bool = False,
    ) -> None:
        ErrorMessage.method_not_implemented_error(name="quantile", class_="Expanding")

    def expanding_sem(
        self,
        fold_axis: Union[int, str],
        expanding_kwargs: dict,
        ddof: int = 1,
        numeric_only: bool = False,
    ) -> "SnowflakeQueryCompiler":
        return self._window_agg(
            window_func=WindowFunction.EXPANDING,
            agg_func="sem",
            window_kwargs=expanding_kwargs,
            agg_kwargs=dict(ddof=ddof, numeric_only=numeric_only),
        )

    def expanding_rank(
        self,
        fold_axis: Union[int, str],
        expanding_kwargs: dict,
        method: str = "average",
        ascending: bool = True,
        pct: bool = False,
        numeric_only: bool = False,
    ) -> None:
        ErrorMessage.method_not_implemented_error(name="rank", class_="Expanding")

    def replace(
        self,
        to_replace: Union[str, int, float, ListLike, dict] = None,
        value: Union[Scalar, ListLike, dict] = lib.no_default,
        limit: Optional[int] = None,
        regex: Union[bool, str, int, float, ListLike, dict] = False,
        method: Union[str, lib.NoDefault] = lib.no_default,
    ) -> "SnowflakeQueryCompiler":
        """
        Replace values given in `to_replace` by `value`.

        Args:
            to_replace: How to find values that will be replaced.
            value: Value to replace any values matching `to_replace` with.
            limit: Not implemented.
            regex: bool or same types as `to_replace`, default False
              Whether to interpret `to_replace` and/or `value` as regular
              expressions. Alternatively, this could be a regular expression or a
              list, dict, or array of regular expressions in which case
              `to_replace` must be ``None``.
            method: Not implemented.

        Returns:
            SnowflakeQueryCompiler with all `to_replace` values replaced by `value`.
        """
        # Propagating client-side types through replace() is complicated.
        # Timedelta columns may change types after replace(), and non-timedelta
        # columns may contain timedelta columns after replace().
        self._raise_not_implemented_error_for_timedelta()

        if method is not lib.no_default:
            ErrorMessage.not_implemented(
                "Snowpark pandas replace API does not support 'method' parameter"
            )

        if limit is not None:
            ErrorMessage.not_implemented(
                "Snowpark pandas replace API does not support 'limit' parameter"
            )

        if value is lib.no_default and not is_dict_like(to_replace) and regex is False:
            raise ValueError(
                f"{type(self).__name__}.replace without 'value' and with non-dict-like "
                "'to_replace' is not supported. Explicitly specify the new values "
                "instead."
            )

        if not (
            is_scalar(to_replace)
            or is_re_compilable(to_replace)
            or is_list_like(to_replace)
        ):

            raise TypeError(
                "Expecting 'to_replace' to be either a scalar, array-like, "
                "dict, or None, got invalid type "
                f"{type(to_replace).__name__!r}"
            )

        if not is_bool(regex):
            if to_replace is not None:
                raise ValueError("'to_replace' must be 'None' if 'regex' is not a bool")
            logging.warning(
                "Regex substitution is performed under the hood using "
                "Snowflake backend. Which supports POSIX ERE syntax for "
                "regular expressions. Please check usage notes for details"
                " https://docs.snowflake.com/en/sql-reference/functions-regexp#general-usage-notes"
            )
            to_replace = regex
            regex = True

        # Convert 'to_replace' to canonically represent a dictionary, where key
        # is column identifier and value is list of values to be replaced.
        replace_map = {}
        value_map = {}
        identifiers = self._modin_frame.data_column_snowflake_quoted_identifiers
        if is_scalar(to_replace):
            replace_map = {i: to_replace for i in identifiers}
        elif is_dict_like(to_replace):
            dict_keys = list(to_replace.keys())  # type: ignore
            dict_values = list(to_replace.values())  # type: ignore

            # Nested dictionary
            if value == lib.no_default and all(is_dict_like(v) for v in dict_values):
                # Keys corresponds to column labels and values corresponds to
                # to_replace to use for that column.
                for label, ids in zip(
                    dict_keys,
                    self._modin_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                        dict_keys, include_index=False
                    ),
                ):
                    for identifier in ids:
                        dict_value = to_replace[label]  # type: ignore
                        replace_map[identifier] = list(dict_value.keys())  # type: ignore
                        value_map[identifier] = list(dict_value.values())  # type: ignore
            elif value == lib.no_default:
                # If value is not provided and to_replace is a dict. dictionary values
                # should be treated as replacement values.
                replace_map = {i: dict_keys for i in identifiers}
                value_map = {i: dict_values for i in identifiers}
            else:
                # if value is provided, keys corresponds to column labels and dictionary
                # values corresponds to to_replace to use for that column.
                for label, ids in zip(
                    dict_keys,
                    self._modin_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                        dict_keys, include_index=False
                    ),
                ):
                    for identifier in ids:
                        replace_map[identifier] = to_replace[label]  # type: ignore
        elif is_list_like(to_replace):
            replace_map = {i: to_replace for i in identifiers}
        else:
            raise TypeError(f"Unsupported to_replace type: {type(to_replace)}")

        # Convert 'value' to canonically represent a dictionary, where
        # key is column identifiers and value is list of values to be used as
        # replacements.
        if is_scalar(value):
            value_map = {i: value for i in identifiers}  # type: ignore
        elif is_dict_like(value):
            # Keys corresponds to column labels and values corresponds to
            # replacement value to use for that column.
            labels = list(value.keys())
            for label, ids in zip(
                labels,
                self._modin_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                    labels, include_index=False
                ),
            ):
                for identifier in ids:
                    value_map[identifier] = value[label]
        elif is_list_like(value):
            value_map = {i: value for i in identifiers}  # type: ignore
        elif value != lib.no_default:
            raise TypeError(f"Unsupported value type: {type(value)}")

        def _scalar_belongs_to_timedelta_classes(s: Any) -> bool:
            return any(
                issubclass(type(s), timedelta_class)
                for timedelta_class in TimedeltaType.types_to_convert_with_from_pandas
            )

        # Raise if the new values in `value` include timedelta.
        if any(
            (
                isinstance(v, list)
                and any(_scalar_belongs_to_timedelta_classes(vv) for vv in v)
            )
            or _scalar_belongs_to_timedelta_classes(v)
            for v in value_map.values()
        ):
            ErrorMessage.not_implemented_for_timedelta("replace")

        replaced_column_exprs = {}
        for identifier, to_replace in replace_map.items():
            if identifier not in value_map:
                continue
            value = value_map[identifier]
            if (
                is_list_like(to_replace)
                and is_list_like(value)
                and len(to_replace) != len(value)  # type: ignore
            ):
                raise ValueError(
                    f"Replacement lists must match in length. Expecting {len(to_replace)} got {len(value)} "  # type: ignore
                )
            if is_scalar(to_replace):
                to_replace = [to_replace]
            if is_scalar(value):
                value = [value] * len(to_replace)  # type: ignore
            column = col(identifier)
            expr: Optional[CaseExpr] = None
            for k, v in zip(to_replace, value):  # type: ignore
                v = pandas_lit(v)
                if native_pd.isna(k):
                    cond = column.is_null()
                elif regex is True:
                    cond = column.regexp(pandas_lit(f".*({k}).*"))
                    v = regexp_replace(subject=column, pattern=k, replacement=v)
                else:
                    cond = column == k
                expr = when(cond, v) if expr is None else expr.when(cond, v)
            expr = expr.otherwise(column) if expr is not None else expr
            replaced_column_exprs[identifier] = expr

        result = self._modin_frame.update_snowflake_quoted_identifiers_with_expressions(
            replaced_column_exprs
        )
        return SnowflakeQueryCompiler(result.frame)

    def add_prefix(
        self, substring: Any, axis: Optional[int] = 0
    ) -> "SnowflakeQueryCompiler":
        return self.add_substring(str(substring), "prefix", axis)

    def add_suffix(
        self, substring: Any, axis: Optional[int] = 0
    ) -> "SnowflakeQueryCompiler":
        return self.add_substring(str(substring), "suffix", axis)

    @snowpark_pandas_type_immutable_check
    def add_substring(
        self,
        substring: str,
        substring_type: Literal["prefix", "suffix"],
        axis: Optional[int] = 0,
    ) -> "SnowflakeQueryCompiler":
        """
        Add a substring to the current row or column labels.

        Parameters
        ----------
        substring : str
            The substring to add.
        substring_type : {"prefix", "suffix"}
            Whether to treat the substring as a prefix or a suffix.
        axis : int
            The axis to update.

        Returns
        -------
        SnowflakeQueryCompiler
            The new query compiler with substring added.
        """
        frame = self._modin_frame
        data_column_pandas_labels = frame.data_column_pandas_labels
        data_column_snowflake_quoted_identifiers = (
            frame.data_column_snowflake_quoted_identifiers
        )

        # Compute prefix + field_name + suffix for both add_prefix and add_suffix.
        prefix = substring if substring_type == "prefix" else ""
        suffix = substring if substring_type == "suffix" else ""

        if axis == 1:
            # This is the case for DataFrame.add_prefix/DataFrame.add_suffix where the column labels are modified.
            if self._modin_frame.is_multiindex(axis=1):
                # If the columns are a MultiIndex, the column labels are tuples. In this case the prefix/suffix is added
                # to each element in the tuple. For instance, for a DataFrame df:
                # >>> df
                # +---------+------------+------------+------------+------------+
                # | row_pos | (bar, one) | (bar, two) | (foo, one) | (foo, two) |
                # +---------+------------+------------+------------+------------+
                # |       0 |          1 |        1.1 |       True |          a |
                # |       1 |          2 |        2.2 |      False |          b |
                # +---------+------------+------------+------------+------------+
                # >>> df.add_prefix("pre_")
                # +---------+--------------------+--------------------+--------------------+--------------------+
                # | row_pos | (pre_bar, pre_one) | (pre_bar, pre_two) | (pre_foo, pre_one) | (pre_foo, pre_two) |
                # +---------+--------------------+--------------------+--------------------+--------------------+
                # |       0 |                  1 |                1.1 |               True |                  a |
                # |       1 |                  2 |                2.2 |              False |                  b |
                # +---------+--------------------+--------------------+--------------------+--------------------+
                # >>> df.add_suffix("_suf")
                # +---------+--------------------+--------------------+--------------------+--------------------+
                # | row_pos | (bar_suf, one_suf) | (bar_suf, two_suf) | (foo_suf, one_suf) | (foo_suf, two_suf) |
                # +---------+--------------------+--------------------+--------------------+--------------------+
                # |       0 |                  1 |                1.1 |               True |                  a |
                # |       1 |                  2 |                2.2 |              False |                  b |
                # +---------+--------------------+--------------------+--------------------+--------------------+
                new_data_column_pandas_labels = []
                for tuple_label in data_column_pandas_labels:
                    new_tuple_label = tuple(
                        prefix + str(label) + suffix for label in tuple_label
                    )
                    new_data_column_pandas_labels.append(new_tuple_label)
                data_column_pandas_labels = new_data_column_pandas_labels
            else:
                # This is the case where the column labels are scalar.
                data_column_pandas_labels = [
                    prefix + str(label) + suffix for label in data_column_pandas_labels
                ]

        result_frame = InternalFrame.create(
            ordered_dataframe=frame.ordered_dataframe,
            data_column_pandas_labels=data_column_pandas_labels,
            data_column_pandas_index_names=frame.data_column_pandas_index_names,
            data_column_snowflake_quoted_identifiers=data_column_snowflake_quoted_identifiers,
            index_column_pandas_labels=frame.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=frame.index_column_snowflake_quoted_identifiers,
            data_column_types=frame.cached_data_column_snowpark_pandas_types,
            index_column_types=frame.cached_index_column_snowpark_pandas_types,
        )

        if axis == 0:
            # This is the case for Series.add_prefix/Series.add_suffix where the index labels are modified. The index in
            # result_frame needs to be updated.
            index_column_quoted_identifiers = (
                result_frame.index_column_snowflake_quoted_identifiers
            )
            # Map from a columns' snowflake quoted identifier to the prefix column expression. Each of these columns is
            # explicitly cast to the string type to prevent type casting exceptions.
            quoted_identifier_to_column_map = {}
            # If the index is a MultiIndex, each level is a column in the Snowflake table, and the prefix/suffix
            # operation is performed on all levels. For instance, for a Series ser with a two-level MultiIndex:
            # >>> ser
            # +--------+--------+------+
            # | level0 | level1 | data |
            # +--------+--------+------+
            # |      0 |      a |  1.1 |
            # |      1 |      b |  2.2 |
            # +--------+--------+------+
            # >>> ser.add_prefix("pre_")
            # +--------+--------+------+
            # | level0 | level1 | data |
            # +--------+--------+------+
            # |  pre_0 |  pre_a |  1.1 |
            # |  pre_1 |  pre_b |  2.2 |
            # +--------+--------+------+
            # >>> ser.add_suffix("_suf")
            # +--------+--------+------+
            # | level0 | level1 | data |
            # +--------+--------+------+
            # |  0_suf |  a_suf |  1.1 |
            # |  1_suf |  b_suf |  2.2 |
            # +--------+--------+------+
            num_levels = result_frame.num_index_columns
            for level in range(num_levels):
                level_identifier = index_column_quoted_identifiers[level]
                original_string = col(level_identifier).cast("string")
                new_string = (
                    [pandas_lit(prefix), original_string]
                    if prefix
                    else [original_string, pandas_lit(suffix)]
                )
                quoted_identifier_to_column_map[level_identifier] = concat(*new_string)
            # Get the new result frame with updated index.
            result_frame = (
                result_frame.update_snowflake_quoted_identifiers_with_expressions(
                    quoted_identifier_to_column_map
                ).frame
            )

        # Returning the query compiler with updated columns and index.
        return SnowflakeQueryCompiler(result_frame)

    def duplicated(
        self,
        subset: Union[Hashable, Sequence[Hashable]] = None,
        keep: DropKeep = "first",
    ) -> "SnowflakeQueryCompiler":
        """
        Return boolean Series denoting duplicate rows.

        Parameters
        ----------
        subset : column label or sequence of labels, optional
            Unused, accepted for compatibility with modin frontend.
            Only consider certain columns for identifying duplicates, by
            default use all the columns; this filtering is already performed on the frontend.
        keep : {'first', 'last', False}, default 'first'
            Determines which duplicates (if any) to mark.

            - ``first`` : Mark duplicates as ``True`` except for the first occurrence.
            - ``last`` : Mark duplicates as ``True`` except for the last occurrence.
            - False : Mark all duplicates as ``True``.

        Returns
        -------
        Snowpark pandas :class:`~modin.pandas.Series`
            Boolean series for each duplicated rows.
        """
        frame = self._modin_frame.ensure_row_position_column()

        # When frame has no data columns, the result should be an empty series of dtype bool,
        # which is internally represented as an empty dataframe with only the MODIN_UNNAMED_SERIES_LABEL column
        if frame.data_column_snowflake_quoted_identifiers == []:
            return SnowflakeQueryCompiler.from_pandas(
                native_pd.DataFrame({MODIN_UNNAMED_SERIES_LABEL: []}, dtype=bool)
            )

        # The main idea is that we:
        # First create a frame, which represents the list of row positions corresponding to the non-duplicate rows.
        # Then, we outer join this frame with the input frame.
        # And finally, we create the output frame which has a single boolean data column whose value depends on
        # whether the non-duplicate row position is present or not.

        row_position_post_dedup_quoted_identifier = (
            frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
                pandas_labels=["row_position_post_dedup"],
            )[0]
        )

        if keep in ["first", "last"]:

            # For first and last, the list of positions of non-duplicate rows is computed using the window funcions
            # first_value and last_value, while paritioning by all data columns.

            if keep == "first":
                func = first_value
            else:
                assert keep == "last"
                func = last_value
            row_position_post_dedup = get_distinct_rows(
                frame.ordered_dataframe.select(
                    func(col(frame.row_position_snowflake_quoted_identifier))
                    .over(
                        Window.partition_by(
                            frame.data_column_snowflake_quoted_identifiers
                        ).order_by(frame.row_position_snowflake_quoted_identifier)
                    )
                    .as_(row_position_post_dedup_quoted_identifier)
                )
            )
        else:
            assert keep is False

            # For keep=False, we cannot use window functions as before because we want to completely drop the
            # partitions/groups representing duplicate rows. For this purpose we use group_by and count aggregation,
            # such that only the groups with count=1 (non-duplicates) are kept.

            row_position_post_dedup = (
                frame.ordered_dataframe.group_by(
                    frame.data_column_snowflake_quoted_identifiers,
                    min_(col(frame.row_position_snowflake_quoted_identifier)).as_(
                        row_position_post_dedup_quoted_identifier
                    ),
                    count(col("*")).as_("count"),
                )
                .filter(col("count") == 1)
                .select(row_position_post_dedup_quoted_identifier)
            )

        row_position_post_dedup = row_position_post_dedup.ensure_row_position_column()
        row_position_post_dedup_frame = InternalFrame.create(
            ordered_dataframe=row_position_post_dedup,
            data_column_pandas_labels=["row_position_post_dedup"],
            data_column_snowflake_quoted_identifiers=[
                row_position_post_dedup_quoted_identifier
            ],
            data_column_pandas_index_names=frame.data_column_pandas_index_names,
            index_column_pandas_labels=[None],
            index_column_snowflake_quoted_identifiers=[
                row_position_post_dedup.row_position_snowflake_quoted_identifier
            ],
            data_column_types=[None],
            index_column_types=[None],
        )

        joined_ordered_dataframe = join_utils.join(
            left=frame,
            right=row_position_post_dedup_frame,
            left_on=[frame.row_position_snowflake_quoted_identifier],
            right_on=[row_position_post_dedup_quoted_identifier],
            how="outer",
        ).result_frame.ordered_dataframe

        duplicated_quoted_identifier = (
            frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
                pandas_labels=["duplicated"],
            )[0]
        )

        new_col = iff(
            col(row_position_post_dedup_quoted_identifier).is_null(),
            pandas_lit(True),
            pandas_lit(False),
        ).as_(duplicated_quoted_identifier)

        new_ordered_dataframe = joined_ordered_dataframe.select(
            frame.index_column_snowflake_quoted_identifiers + [new_col]
        )
        new_frame = InternalFrame.create(
            ordered_dataframe=new_ordered_dataframe,
            data_column_pandas_labels=[MODIN_UNNAMED_SERIES_LABEL],
            data_column_snowflake_quoted_identifiers=[duplicated_quoted_identifier],
            data_column_pandas_index_names=frame.data_column_pandas_index_names,
            index_column_pandas_labels=frame.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=frame.index_column_snowflake_quoted_identifiers,
            data_column_types=[None],
            index_column_types=frame.cached_index_column_snowpark_pandas_types,
        )

        return SnowflakeQueryCompiler(new_frame)

    def _binary_op_between_dataframe_and_series_along_axis_0(
        self,
        op: str,
        other: "SnowflakeQueryCompiler",
        fill_value: Optional[Scalar] = None,
    ) -> "SnowflakeQueryCompiler":
        """
        Computes binary operation between DataFrame (self) and Series (other).

        Example:
            To compute the result of df + s
            where df = pd.DataFrame({'A': [4,6,None,7], 'B': [2,5,4,None]})

            |    |   A |   B |
            |----|-----|-----|
            |  0 |   4 |   2 |
            |  1 |   6 |   5 |
            |  2 | nan |   4 |
            |  3 |   7 | nan |

            and s = pd.Series([9, 10, 12], index=[3, 1, 4])

            |    |   0 |
            |----|-----|
            |  3 |   9 |
            |  1 |  10 |
            |  4 |  12 |

            the result is

            |    |   A |   B |
            |----|-----|-----|
            |  0 | nan | nan |
            |  1 |  16 |  15 |
            |  2 | nan | nan |
            |  3 |  16 | nan |
            |  4 | nan | nan |

            pandas first aligns the index of the Dataframe with the Series, and then carries the operation only out for any
            rows with matching indices. This makes the operation similar to an outer join.
            Applying the binary operation will preserve null values like in SQL. Unmatched rows in pandas
            are considered NaN.

        Args:
            op: string identifying operation to carry out.
            other: the right side operand, a SnowflakeQueryCompiler representing a Series.
            fill_value: optional fill_value

        Returns:
            SnowflakeQueryCompiler representing result of binary op operation.
        """
        assert (
            other.is_series_like()
        ), "other must be a Snowflake Query Compiler representing a Series"

        # pandas does not support fill_value for this scenario, raise compatible NotImplementedError here.
        # This behavior exists also for pandas 2.0.3.
        if fill_value is not None:
            # code pointer: pandas/core/ops/__init__.py:L431 for 1.5.x, left as TODO
            raise NotImplementedError(f"fill_value {fill_value} not supported.")

        left_index_columns = self._modin_frame.index_column_snowflake_quoted_identifiers
        right_index_columns = (
            other._modin_frame.index_column_snowflake_quoted_identifiers
        )

        left_data_columns = self._modin_frame.data_column_snowflake_quoted_identifiers
        right_data_columns = other._modin_frame.data_column_snowflake_quoted_identifiers

        coalesce_config = [JoinKeyCoalesceConfig.LEFT] * len(left_index_columns)

        joined_frame = join_utils.join(
            self._modin_frame,
            other._modin_frame,
            how="outer",
            left_on=left_index_columns,
            right_on=right_index_columns,
            sort=True,
            join_key_coalesce_config=coalesce_config,
            inherit_join_index=InheritJoinIndex.FROM_BOTH,
        )
        left_result_data_identifiers = (
            joined_frame.result_column_mapper.map_left_quoted_identifiers(
                left_data_columns
            )
        )
        right_result_data_identifiers = (
            joined_frame.result_column_mapper.map_right_quoted_identifiers(
                right_data_columns
            )
        )

        # Lazify type map here for calling binaryOp.compute.
        def create_lazy_type_functions(
            identifiers: list[str],
        ) -> list[DataTypeGetter]:
            """
            create functions that return datatype on demand for an identifier.
            Args:
                identifiers: List of Snowflake quoted identifiers

            Returns:
                List of callables to enable lazy on-demand datatype retrieval.
            """
            return [
                lambda identifier=identifier: joined_frame.result_frame.get_snowflake_type(  # type: ignore[misc]
                    identifier
                )
                for identifier in identifiers
            ]

        left_datatypes = create_lazy_type_functions(left_result_data_identifiers)
        right_datatypes = create_lazy_type_functions(right_result_data_identifiers)

        # Right must be a Series, so there should be a single data column
        assert len(right_result_data_identifiers) == 1, "other must be a Series"
        right = right_result_data_identifiers[0]
        right_datatype = right_datatypes[0]
        # now replace in result frame identifiers with binary op result
        replace_mapping = {}
        snowpark_pandas_types = []
        for left, left_datatype in zip(left_result_data_identifiers, left_datatypes):
            (expression, snowpark_pandas_type,) = BinaryOp.create(
                op, col(left), left_datatype, col(right), right_datatype
            ).compute()
            snowpark_pandas_types.append(snowpark_pandas_type)
            replace_mapping[left] = expression
        update_result = joined_frame.result_frame.update_snowflake_quoted_identifiers_with_expressions(
            replace_mapping, snowpark_pandas_types
        )
        new_frame = update_result.frame

        # keep only index columns and left identifiers (drop right, which stem from Series)
        identifiers_to_keep = set(
            new_frame.index_column_snowflake_quoted_identifiers
        ) | set(update_result.old_id_to_new_id_mappings.values())
        self_is_column_mi = len(self._modin_frame.data_column_pandas_index_names)
        label_to_snowflake_quoted_identifier = []
        snowflake_quoted_identifier_to_snowpark_pandas_type = {}
        for pair in new_frame.label_to_snowflake_quoted_identifier:
            if pair.snowflake_quoted_identifier in identifiers_to_keep:
                if (
                    self_is_column_mi
                    and isinstance(pair.label, tuple)
                    and isinstance(pair.label[0], tuple)
                ):
                    pair = LabelIdentifierPair(
                        pair.label[0], pair.snowflake_quoted_identifier
                    )
                label_to_snowflake_quoted_identifier.append(pair)
                snowflake_quoted_identifier_to_snowpark_pandas_type[
                    pair.snowflake_quoted_identifier
                ] = new_frame.snowflake_quoted_identifier_to_snowpark_pandas_type[
                    pair.snowflake_quoted_identifier
                ]

        new_frame = InternalFrame(
            ordered_dataframe=new_frame.ordered_dataframe,
            label_to_snowflake_quoted_identifier=tuple(
                label_to_snowflake_quoted_identifier
            ),
            num_index_columns=new_frame.num_index_columns,
            data_column_index_names=self._modin_frame.data_column_index_names,
            snowflake_quoted_identifier_to_snowpark_pandas_type=snowflake_quoted_identifier_to_snowpark_pandas_type,
        )

        return SnowflakeQueryCompiler(new_frame)

    def round(
        self, decimals: Union[int, Mapping, "pd.Series"] = 0, **kwargs: Any
    ) -> "SnowflakeQueryCompiler":
        """
        Round every numeric value up to specified number of decimals.

        Parameters
        ----------
        decimals : int or list-like
            Number of decimals to round each column to.
        **kwargs : dict
            Serves the compatibility purpose. Does not affect the result.

        Returns
        -------
        BaseQueryCompiler
            QueryCompiler with rounded values.
        """
        # DataFrame.round() and Series.round() ignore non-numeric columns like
        # timedelta. We raise a Snowflake error for non-numeric, non-timedelta
        # columns like strings, but we have to detect timedelta separately
        # because its underlying representation is an integer. Without this
        # check, we'd round the integer representation of the timedelta instead
        # of leaving the timedelta unchanged.
        self._raise_not_implemented_error_for_timedelta()

        if isinstance(decimals, pd.Series):
            raise ErrorMessage.not_implemented(
                "round with decimals of type Series is not yet supported"
            )

        if isinstance(decimals, dict):
            decimals_keys = list(decimals.keys())
            id_to_decimal_dict = {}
            for label, ids in zip(
                decimals_keys,
                self._modin_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                    decimals_keys, include_index=False
                ),
            ):
                for id in ids:
                    id_to_decimal_dict[id] = decimals[label]

        def round_col(column: SnowparkColumn) -> SnowparkColumn:
            if is_scalar(decimals):
                return snowpark_round(column, decimals)
            elif is_dict_like(decimals):
                if column.get_name() in id_to_decimal_dict:
                    return snowpark_round(column, id_to_decimal_dict[column.get_name()])
                else:
                    return column

        new_internal_frame = self._modin_frame.apply_snowpark_function_to_columns(
            lambda column: round_col(column)
        )

        return SnowflakeQueryCompiler(new_internal_frame)

    def idxmax(
        self,
        axis: int = 0,
        skipna: bool = True,
        numeric_only: bool = False,
    ) -> "SnowflakeQueryCompiler":
        """
        Return index of first occurrence of maximum over requested axis.

        Args:
            axis : {0 or 1}, default 0
                The axis to use. 0 for row-wise, 1 for column-wise.
            skipna : bool, default True
                Exclude NA/null values. If an entire row/column is NA, the result will be NA.
            numeric_only: bool, default False:
                Include only float, int or boolean data.

        Returns:
            SnowflakeQueryCompiler
        """
        return self._idxmax_idxmin(
            func="idxmax", axis=axis, skipna=skipna, numeric_only=numeric_only
        )

    def idxmin(
        self,
        axis: int = 0,
        skipna: bool = True,
        numeric_only: bool = False,
    ) -> "SnowflakeQueryCompiler":
        """
        Return index of first occurrence of minimum over requested axis.

        Args:
            axis : {0 or 1}, default 0
                The axis to use. 0 for row-wise, 1 for column-wise.
            skipna : bool, default True
                Exclude NA/null values. If an entire row/column is NA, the result will be NA.
            numeric_only: bool, default False:
                Include only float, int or boolean data.

        Returns:
            SnowflakeQueryCompiler
        """
        return self._idxmax_idxmin(
            func="idxmin", axis=axis, skipna=skipna, numeric_only=numeric_only
        )

    def _idxmax_idxmin(
        self,
        func: AggFuncType,
        axis: int = 0,
        skipna: bool = True,
        numeric_only: bool = False,
    ) -> "SnowflakeQueryCompiler":
        """
        Return index of first/last occurrence of maximum over requested axis.

        Args:
            func: {"idxmax" or "idxmin"}
            axis : {0 or 1}, default 0
                The axis to use. 0 for row-wise, 1 for column-wise.
            skipna : bool, default True
                Exclude NA/null values. If an entire row/column is NA, the result will be NA.
            numeric_only: bool, default False:
                Include only float, int or boolean data.

        Returns:
            SnowflakeQueryCompiler
        """
        return self.agg(
            func=func,
            axis=axis,
            args=[],
            kwargs={
                "numeric_only": numeric_only,
                "skipna": skipna,
            },
        ).set_columns([None])

    def _binary_op_between_dataframes(
        self, op: str, other: "SnowflakeQueryCompiler", fill_value: Optional[Scalar]
    ) -> "SnowflakeQueryCompiler":
        """
        Compute binary operation between self and other, which both represent a DataFrame.
        Args:
            op: operation to carry out
            other: the rhs when applying the binary op
            fill_value: an optional fill_value

        Returns:
            SnowflakeQueryCompiler representing a DataFrame holding the result.
        """

        def infer_sorted_column_labels(
            lhs_data_column_labels: list[Hashable],
            rhs_data_column_labels: list[Hashable],
        ) -> list[Hashable]:
            """
            Helper function to infer the column labels after combining two Dataframes. pandas does not follow
            np.sort() or sorted(...) or sorted(..., key=lambda x: str(x)). In order to stay compatible with future pandas
            versions infer order through pandas itself within this function.
            Args:
                lhs_data_column_labels: column labels of the left Dataframe, i.e. a list representing the values of DataFrame.columns.
                rhs_data_column_labels: column labels of the right Dataframe, i.e. a list representing the values of DataFrame.columns.

            Returns:
                List of column labels of the combined Dataframe that would be the result of DataFrame <op> Series (or vice-versa).
            """

            # The column labels of the result Dataframe are independent of which binop is used.
            # Create a dummy Dataframe with a single row of 0s and a dummy Series of 0s.
            # Then apply a binary operation (here +), and retrieve the result columns.

            lhs = native_pd.DataFrame(
                data=[[0] * len(lhs_data_column_labels)],
                columns=lhs_data_column_labels,
            )
            rhs = native_pd.DataFrame(
                data=[[0] * len(rhs_data_column_labels)],
                columns=rhs_data_column_labels,
            )

            combined_df = lhs + rhs
            return list(combined_df.columns.values)

        self_frame = self._modin_frame
        other_frame = other._modin_frame

        # pandas throws an incomprehensible error
        # AssertionError: Gaps in blk ref_locs
        # when either self_frame or other_frame have duplicate labels.
        # Deviate here from pandas behavior, and throw an error similar to Series/Dataframe by surfacing
        # to the other duplicate labels.
        # Asserting this condition allows to simplify code below.
        if not is_duplicate_free(
            self_frame.data_column_pandas_labels
        ) or not is_duplicate_free(other_frame.data_column_pandas_labels):
            raise ValueError("cannot reindex on an axis with duplicate labels")

        combined_data_labels = infer_sorted_column_labels(
            self_frame.data_column_pandas_labels, other_frame.data_column_pandas_labels
        )

        # Align (join) both dataframes on columns and index.
        align_result = join_utils.align(
            left=self_frame,
            right=other_frame,
            left_on=self_frame.index_column_snowflake_quoted_identifiers,
            right_on=other_frame.index_column_snowflake_quoted_identifiers,
            how="outer",
        )

        left_right_pairs = prepare_binop_pairs_between_dataframe_and_dataframe(
            align_result, combined_data_labels, self_frame, other_frame
        )

        replace_mapping = {}
        data_column_snowpark_pandas_types = []
        for p in left_right_pairs:
            result_expression, snowpark_pandas_type = BinaryOp.create_with_fill_value(
                op=op,
                lhs=p.lhs,
                lhs_datatype=p.lhs_datatype,
                rhs=p.rhs,
                rhs_datatype=p.rhs_datatype,
                fill_value=fill_value,
            ).compute()
            replace_mapping[p.identifier] = result_expression
            data_column_snowpark_pandas_types.append(snowpark_pandas_type)
        # Create restricted frame with only combined / replaced labels.
        updated_result = align_result.result_frame.update_snowflake_quoted_identifiers_with_expressions(
            replace_mapping
        )
        updated_data_identifiers = [
            updated_result.old_id_to_new_id_mappings[p.identifier]
            for p in left_right_pairs
        ]
        new_frame = updated_result.frame
        result_frame = InternalFrame.create(
            ordered_dataframe=new_frame.ordered_dataframe,
            data_column_pandas_labels=combined_data_labels,
            data_column_pandas_index_names=new_frame.data_column_pandas_index_names,
            data_column_snowflake_quoted_identifiers=updated_data_identifiers,
            index_column_pandas_labels=new_frame.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=new_frame.index_column_snowflake_quoted_identifiers,
            data_column_types=data_column_snowpark_pandas_types,
            index_column_types=None,
        )

        return SnowflakeQueryCompiler(result_frame)

    def _binary_op_between_dataframe_and_series_along_axis_1(
        self,
        op: str,
        other: "SnowflakeQueryCompiler",
        squeeze_self: bool,
        fill_value: Optional[Scalar] = None,
    ) -> "SnowflakeQueryCompiler":
        """
        Compute result of DataFrame and Series (or vice-versa) along axis=1 (row-wise).
        Args:
            other: A Dataframe or Series
            squeeze_self: indicates whether self is a series. If true, then self is a series.
            fill_value: Optional fill-value, default None.

        Returns:
            SnowflakeQueryCompiler representing result of binop along axis 1.
        """

        # Applying binary operations along axis=1 works in pandas by aligning the index of the Series
        # with the column labels (column index) of the DataFrame. The result will be a DataFrame. There is no
        # obvious pattern on how the columns are sorted, so in this implementation the sorting order is inferred
        # from pandas itself to stay compatible.
        # Example:
        # df = pd.DataFrame([[10, None, 20, None], [None, None, None, None]], columns=[1, 3, 4, 5])
        # |    |   1 | 3   |   4 | 5   |
        # |----|-----|-----|-----|-----|
        # |  0 |  10 |     |  20 |     |
        # |  1 | nan |     | nan |     |
        #
        # s = pd.Series([None, 1, 2, None, 3, 4, -99])
        # |    |   0 |
        # |----|-----|
        # |  0 | nan |
        # |  1 |   1 |
        # |  2 |   2 |
        # |  3 | nan |
        # |  4 |   3 |
        # |  5 |   4 |
        # |  6 | -99 |
        #
        # The result of df.sub(s, axis=1) is
        # |    |   0 |   1 |   2 |   3 |   4 |   5 |   6 |
        # |----|-----|-----|-----|-----|-----|-----|-----|
        # |  0 | nan |   9 | nan | nan |  17 | nan | nan |
        # |  1 | nan | nan | nan | nan | nan | nan | nan |
        # The logic matches the row (1, 1) from the Series to the column indexed by 1 and applies the value from the row (here 1) to
        # each element in the column indexed by 1. In this case, 10 - 1 = 9.
        # Similarly, the row (4, 3) is matched to the column indexed by 4 in the Dataframe. Here applying sub (-) yields
        # 20 - 3 = 17.

        # pandas compatible NotImplementedError
        if fill_value is not None:
            raise NotImplementedError(f"fill_value {fill_value} not supported.")

        def infer_sorted_column_labels(
            data_column_labels: list[Hashable], series: native_pd.Series
        ) -> list[Hashable]:
            """
            Helper function to infer the column labels after combining a Series with a Dataframe. pandas does not follow
            np.sort() or sorted(...) or sorted(..., key=lambda x: str(x)). In order to stay compatible with future pandas
            versions infer order through pandas itself within this function.
            Args:
                data_column_labels: column labels of the Dataframe, i.e. a list representing the values of DataFrame.columns.
                series: Series with which to combine a Dataframe having data_column_labels.

            Returns:
                List of column labels of the combined Dataframe that would be the result of DataFrame <op> Series (or vice-versa).
            """

            # The column labels of the result Dataframe are independent of which binop is used.
            # Create a dummy Dataframe with a single row of 0s and a dummy Series of 0s.
            # Then apply a binary operation (here +), and retrieve the result columns.

            df = native_pd.DataFrame(
                data=[[0] * len(data_column_labels)],
                columns=data_column_labels,
            )
            s = native_pd.Series([0] * len(series), index=series.index)

            combined_df = df + s
            return list(combined_df.columns.values)

        # For whichever side is the Series, collect the data. Alternatively, we could use transpose however the query count would
        # be the same (as a describe needs to be issued to get the schema of the transposed data). To save on transposing
        # and a describe query, directly collect data. We may want to revisit this in the future.
        # Convert index values here to list, because is_duplicate_free does not support numpy arrays.
        # Inherit the index names from the dataframe.
        if squeeze_self:
            # self is a Series, other a DataFrame.
            series_self = self.to_pandas()
            # Series.squeeze on one row returns a scalar, so instead use squeeze with axis=0
            series = (
                series_self.squeeze()
                if series_self.size > 1
                else series_self.squeeze(axis=0)
            )

            self_column_labels = list(series.index.values)
            other_column_labels = other._modin_frame.data_column_pandas_labels
            frame = other._modin_frame
            index_column_pandas_labels = other._modin_frame.index_column_pandas_labels

            sorted_column_labels = infer_sorted_column_labels(
                other._modin_frame.data_column_pandas_labels,
                series,
            )
        else:
            # self is a DataFrame, other a Series.
            series_other = other.to_pandas()
            # Series.squeeze on one row returns a scalar, so instead use squeeze with axis=0
            series = (
                series_other.squeeze()
                if series_other.size > 1
                else series_other.squeeze(axis=0)
            )

            self_column_labels = self._modin_frame.data_column_pandas_labels
            other_column_labels = list(series.index.values)
            frame = self._modin_frame
            index_column_pandas_labels = self._modin_frame.index_column_pandas_labels

            sorted_column_labels = infer_sorted_column_labels(
                self._modin_frame.data_column_pandas_labels,
                series,
            )

        # Align both pandas labels from self and other.
        # pandas produces a ValueError: cannot reindex on an axis with duplicate labels when there are duplicate labels
        # if labels aren't unique. We use this below to optimize and avoid calling (a potentially expensive) align
        # operation between Series and DataFrame.
        if not is_duplicate_free(self_column_labels) or not is_duplicate_free(
            other_column_labels
        ):
            raise ValueError("cannot reindex on an axis with duplicate labels")

        # Add to frame NaN columns for all labels not present.
        missing_labels = list(
            filter(
                lambda label: label not in frame.data_column_pandas_labels,
                sorted_column_labels,
            )
        )
        new_identifiers = frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
            pandas_labels=missing_labels
        )

        expanded_ordered_frame = append_columns(
            frame.ordered_dataframe,
            new_identifiers,
            [pandas_lit(None)] * len(new_identifiers),
        )

        # Short-circuit: If there is no overlap between columns, pandas will append columns of other
        # and every single column will be pandas_lit(None). The order is defined by sorted_column_labels
        if len(set(self_column_labels) & set(other_column_labels)) == 0:
            new_frame = InternalFrame.create(
                ordered_dataframe=expanded_ordered_frame,
                data_column_pandas_labels=sorted_column_labels,
                data_column_pandas_index_names=self._modin_frame.data_column_pandas_index_names,
                data_column_snowflake_quoted_identifiers=frame.data_column_snowflake_quoted_identifiers
                + new_identifiers,
                index_column_pandas_labels=index_column_pandas_labels,
                index_column_snowflake_quoted_identifiers=frame.index_column_snowflake_quoted_identifiers,
                data_column_types=None,
                index_column_types=None,
            )
            # Replace all columns with NULL literals.
            new_frame = new_frame.update_snowflake_quoted_identifiers_with_expressions(
                {
                    identifier: pandas_lit(None)
                    for identifier in new_frame.data_column_snowflake_quoted_identifiers
                }
            ).frame

            return SnowflakeQueryCompiler(new_frame)

        # Regular case: There are overlapping columns/rows for which a computation needs to be carried out.
        q_frame = sorted(
            list(
                zip(
                    frame.data_column_pandas_labels,
                    frame.data_column_snowflake_quoted_identifiers,
                )
            ),
            key=lambda t: sorted_column_labels.index(t[0]),
        )

        q_missing = sorted(
            list(zip(missing_labels, new_identifiers)),
            key=lambda t: sorted_column_labels.index(t[0]),
        )

        pairs = merge_label_and_identifier_pairs(
            sorted_column_labels, q_frame, q_missing
        )

        expanded_data_column_pandas_labels = list(map(lambda t: t[0], pairs))
        expanded_data_column_snowflake_quoted_identifiers = list(
            map(lambda t: t[1], pairs)
        )

        # Create new InternalFrame with updated mapping.
        new_frame = InternalFrame.create(
            ordered_dataframe=expanded_ordered_frame,
            data_column_pandas_labels=expanded_data_column_pandas_labels,
            data_column_pandas_index_names=self._modin_frame.data_column_pandas_index_names,
            data_column_snowflake_quoted_identifiers=expanded_data_column_snowflake_quoted_identifiers,
            index_column_pandas_labels=index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=frame.index_column_snowflake_quoted_identifiers,
            data_column_types=[
                frame.snowflake_quoted_identifier_to_snowpark_pandas_type.get(
                    identifier
                )
                for identifier in expanded_data_column_snowflake_quoted_identifiers
            ],
            index_column_types=None,
        )

        # For columns that exist in both self and other, update the corresponding identifier with the result
        # of applying a binary operation between both.
        overlapping_pairs = [
            t
            for t in zip(
                new_frame.data_column_pandas_labels,
                new_frame.data_column_snowflake_quoted_identifiers,
            )
            if t[0] in self_column_labels and t[0] in other_column_labels
        ]

        assert len(overlapping_pairs) > 0, "case for no overlapping pairs handled above"

        datatype_getters = {
            identifier: lambda identifier=identifier: new_frame.get_snowflake_type(
                identifier
            )
            for _, identifier in overlapping_pairs
        }

        replace_mapping = {}
        snowpark_pandas_labels = []
        for label, identifier in overlapping_pairs:
            expression, new_type = (
                BinaryOp.create_with_lhs_scalar(
                    op,
                    series.loc[label],
                    col(identifier),
                    datatype_getters[identifier],
                ).compute()
                if squeeze_self
                else BinaryOp.create_with_rhs_scalar(
                    op,
                    col(identifier),
                    datatype_getters[identifier],
                    series.loc[label],
                ).compute()
            )
            snowpark_pandas_labels.append(new_type)
            replace_mapping[identifier] = expression
        return SnowflakeQueryCompiler(
            new_frame.update_snowflake_quoted_identifiers_with_expressions(
                replace_mapping, snowpark_pandas_labels
            ).frame
        )

    def _replace_non_str(
        self,
        in_col: SnowparkColumn,
        out_col: SnowparkColumn,
        replacement_value: Optional[object] = None,
    ) -> SnowparkColumn:
        """
        Handle the case where the input column to the string method may contain mixed types.
        In this case, we follow the pandas behavior, where all non-string input value results
        in a Null value (for most string methods). For some string methods, those resulting
        Null values are replaced by some configured value based on a parameter in the method's
        signature (e.g., `str_contains` has an `na` parameter).

        Parameters
        ----------
        in_col : SnowparkColumn
            Input column to the string method.
        out_col : SnowparkColumn
            Output column from the string method if in_col was not null.
        replacement_value : Optional[str], default None.
            value to use for out_col when the value of in_col is non-string.
        """
        return iff(
            not_(is_char(to_variant(in_col))), pandas_lit(replacement_value), out_col
        )

    def _str_startswith_endswith(
        self,
        pat: Union[str, tuple],
        na: object = None,
        is_startswith: bool = True,
    ) -> "SnowflakeQueryCompiler":
        """
        Test if the start (or end) of each string element matches a pattern.

        Parameters
        ----------
        pat : str or tuple[str, …]
            Character sequence or tuple of strings. Regular expressions are not accepted.
        na : object, default NaN
            Object shown if element tested is not a string. The default depends on dtype of the array. For object-dtype, numpy.nan is used. For StringDtype, pandas.NA is used.
        is_startswith : bool
            True if the string operation is startswith. Otherwise, the string operation is endswith.

        Returns
        -------
        SnowflakeQueryCompiler representing result of the string operation.
        """
        if not native_pd.isna(na) and not isinstance(na, bool):
            ErrorMessage.not_implemented(
                "Snowpark pandas doesn't support non-bool 'na' argument"
            )

        if isinstance(pat, str):
            pat = (pat,)
        if not isinstance(pat, tuple):
            raise TypeError(f"expected a string or tuple, not {type(pat).__name__}")

        def output_col(
            column: SnowparkColumn, pat: tuple, na: object
        ) -> SnowparkColumn:
            if all([not isinstance(p, str) for p in pat]):
                new_col = pandas_lit(np.nan)
            else:
                prefix = "" if is_startswith else "(.|\n)*"
                suffix = "(.|\n)*" if is_startswith else ""
                new_pat = "|".join(
                    f"{prefix}{re.escape(p)}{suffix}" for p in pat if isinstance(p, str)
                )
                new_col = column.rlike(pandas_lit(new_pat))
                if any([not isinstance(p, str) for p in pat]):
                    new_col = iff(new_col, pandas_lit(True), pandas_lit(None))
            new_col = new_col if na is None else coalesce(new_col, pandas_lit(na))
            return self._replace_non_str(column, new_col, replacement_value=na)

        new_internal_frame = self._modin_frame.apply_snowpark_function_to_columns(
            lambda column: output_col(column, pat, na)
        )

        return SnowflakeQueryCompiler(new_internal_frame)

    def str_cat(
        self,
        others: ListLike,
        sep: Optional[str] = None,
        na_rep: Optional[str] = None,
        join: Literal["left", "right", "outer", "inner"] = "left",
    ) -> None:
        ErrorMessage.method_not_implemented_error("cat", "Series.str")

    def str_decode(self, encoding: str, errors: str) -> None:
        ErrorMessage.method_not_implemented_error("decode", "Series.str")

    def str_encode(self, encoding: str, errors: str) -> None:
        ErrorMessage.method_not_implemented_error("encode", "Series.str")

    def str_startswith(
        self, pat: Union[str, tuple], na: object = None
    ) -> "SnowflakeQueryCompiler":
        """
        Test if the start of each string element matches a pattern.

        Parameters
        ----------
        pat : str or tuple[str, …]
            Character sequence or tuple of strings. Regular expressions are not accepted.
        na : object, default NaN
            Object shown if element tested is not a string. The default depends on dtype of the array. For object-dtype, numpy.nan is used. For StringDtype, pandas.NA is used.

        Returns
        -------
        SnowflakeQueryCompiler representing result of the string operation.
        """
        return self._str_startswith_endswith(pat, na, is_startswith=True)

    def str_endswith(
        self, pat: Union[str, tuple], na: object = None
    ) -> "SnowflakeQueryCompiler":
        """
        Test if the end of each string element matches a pattern.

        Parameters
        ----------
        pat : str or tuple[str, …]
            Character sequence or tuple of strings. Regular expressions are not accepted.
        na : object, default NaN
            Object shown if element tested is not a string. The default depends on dtype of the array. For object-dtype, numpy.nan is used. For StringDtype, pandas.NA is used.

        Returns
        -------
        SnowflakeQueryCompiler representing result of the string operation.
        """
        return self._str_startswith_endswith(pat, na, is_startswith=False)

    def str_find(self, sub: str, start: int = 0, end: Optional[int] = None) -> None:
        ErrorMessage.method_not_implemented_error("find", "Series.str")

    def str_rfind(self, sub: str, start: int = 0, end: Optional[int] = None) -> None:
        ErrorMessage.method_not_implemented_error("rfind", "Series.str")

    def str_findall(self, pat: str, flags: int = 0) -> None:
        ErrorMessage.method_not_implemented_error("findall", "Series.str")

    def str_index(self, sub: str, start: int = 0, end: Optional[int] = None) -> None:
        ErrorMessage.method_not_implemented_error("index", "Series.str")

    def str_rindex(self, sub: str, start: int = 0, end: Optional[int] = None) -> None:
        ErrorMessage.method_not_implemented_error("rindex", "Series.str")

    def str_fullmatch(
        self, pat: str, case: bool = True, flags: int = 0, na: object = None
    ) -> None:
        ErrorMessage.method_not_implemented_error("fullmatch", "Series.str")

    def str_match(
        self, pat: str, case: bool = True, flags: int = 0, na: object = None
    ) -> "SnowflakeQueryCompiler":
        """
        Determine if each string starts with a match of a regular expression.

        Parameters
        ----------
        pat : str
            Character sequence.
        case : bool, default True
            If True, case sensitive.
        flags : int, default 0 (no flags)
            Regex module flags, e.g. re.IGNORECASE.
        na : scalar, optional
            Fill value for missing values. The default depends on dtype of the array. For object-dtype, numpy.nan is used. For StringDtype, pandas.NA is used.

        Returns
        -------
        SnowflakeQueryCompiler representing result of the string operation.
        """
        if not native_pd.isna(na) and not isinstance(na, bool):
            ErrorMessage.not_implemented(
                "Snowpark pandas method 'Series.str.match' does not support non-bool 'na' argument"
            )

        pat = f"({pat})(.|\n)*"
        if flags & re.IGNORECASE > 0:
            case = False
        if flags & re.IGNORECASE == 0 and not case:
            flags = flags | re.IGNORECASE
        params = self._get_regex_params(flags)

        def output_col(column: SnowparkColumn, pat: str, na: object) -> SnowparkColumn:
            new_col = builtin("rlike")(column, pandas_lit(pat), pandas_lit(params))
            new_col = (
                new_col if pandas.isnull(na) else coalesce(new_col, pandas_lit(na))
            )
            return self._replace_non_str(column, new_col, replacement_value=na)

        new_internal_frame = self._modin_frame.apply_snowpark_function_to_columns(
            lambda column: output_col(column, pat, na)
        )

        return SnowflakeQueryCompiler(new_internal_frame)

    def str_extract(self, pat: str, flags: int = 0, expand: bool = True) -> None:
        ErrorMessage.method_not_implemented_error("extract", "Series.str")

    def str_extractall(self, pat: str, flags: int = 0, expand: bool = True) -> None:
        ErrorMessage.method_not_implemented_error("extractall", "Series.str")

    def str_capitalize(self) -> "SnowflakeQueryCompiler":
        """
        Capitalize the string

        Returns
        -------
        SnowflakeQueryCompiler representing result of the string operation.
        """

        new_internal_frame = self._modin_frame.apply_snowpark_function_to_columns(
            # We use delimeters and set it as the empty string so that we treat the entire string as one word
            # and thus only capitalize the first character of the first word
            lambda col: self._replace_non_str(
                col, initcap(col, delimiters=pandas_lit(""))
            )
        )
        return SnowflakeQueryCompiler(new_internal_frame)

    def str_isalnum(self) -> None:
        ErrorMessage.method_not_implemented_error("isalnum", "Series.str")

    def str_isalpha(self) -> None:
        ErrorMessage.method_not_implemented_error("isalpha", "Series.str")

    def str_isdigit(self) -> "SnowflakeQueryCompiler":
        """
        Check whether all characters in each string are digits.

        Returns
        -------
        SnowflakeQueryCompiler representing result of the string operation.
        """
        new_internal_frame = self._modin_frame.apply_snowpark_function_to_columns(
            lambda column: self._replace_non_str(column, column.rlike("[0-9]+"))
        )
        return SnowflakeQueryCompiler(new_internal_frame)

    def str_isspace(self) -> None:
        ErrorMessage.method_not_implemented_error("isspace", "Series.str")

    def str_islower(self) -> "SnowflakeQueryCompiler":
        """
        Check whether all characters in each string are lowercase.

        Returns
        -------
        SnowflakeQueryCompiler representing result of the string operation.
        """
        new_internal_frame = self._modin_frame.apply_snowpark_function_to_columns(
            lambda column: self._replace_non_str(
                column,
                column.rlike("(.|\n)*[a-zA-Z]+(.|\n)*").__and__(
                    column.__eq__(lower(column))
                ),
            )
        )
        return SnowflakeQueryCompiler(new_internal_frame)

    def str_isupper(self) -> "SnowflakeQueryCompiler":
        """
        Check whether all characters in each string are uppercase.

        Returns
        -------
        SnowflakeQueryCompiler representing result of the string operation.
        """
        new_internal_frame = self._modin_frame.apply_snowpark_function_to_columns(
            lambda column: self._replace_non_str(
                column,
                column.rlike("(.|\n)*[a-zA-Z]+(.|\n)*").__and__(
                    column.__eq__(upper(column))
                ),
            )
        )
        return SnowflakeQueryCompiler(new_internal_frame)

    def str_istitle(self) -> "SnowflakeQueryCompiler":
        """
        Check whether each string is titlecase.
        We do a regex matching as follows
        ([^a-zA-Z]*[A-Z]{1}[a-z]*([^a-zA-Z]|$)+): matches a title pattern one or more times
        [^a-zA-Z]*: matches any non-alpha character at the beginning
        [A-Z]{1}: matches one uppercase letter
        [a-z]*: match any lowercase letters
        ([^a-zA-Z]|$)+)+$: ignore non-alpha characters at the end

        Returns
        -------
        SnowflakeQueryCompiler representing result of the string operation.
        """
        new_internal_frame = self._modin_frame.apply_snowpark_function_to_columns(
            lambda column: self._replace_non_str(
                column,
                column.rlike("^([^a-zA-Z]*[A-Z]{1}[a-z]*([^a-zA-Z]|$)+)+$"),
            )
        )
        return SnowflakeQueryCompiler(new_internal_frame)

    def str_isnumeric(self) -> None:
        ErrorMessage.method_not_implemented_error("isnumeric", "Series.str")

    def str_isdecimal(self) -> None:
        ErrorMessage.method_not_implemented_error("isdecimal", "Series.str")

    def str_lower(self) -> "SnowflakeQueryCompiler":
        """
        Convert strings to lowercase.

        Returns
        -------
        SnowflakeQueryCompiler representing result of the string operation.
        """
        new_internal_frame = self._modin_frame.apply_snowpark_function_to_columns(
            lambda column: self._replace_non_str(column, lower(column))
        )
        return SnowflakeQueryCompiler(new_internal_frame)

    def str_upper(self) -> "SnowflakeQueryCompiler":
        """
        Convert strings to uppercase.

        Returns
        -------
        SnowflakeQueryCompiler representing result of the string operation.
        """
        new_internal_frame = self._modin_frame.apply_snowpark_function_to_columns(
            lambda column: self._replace_non_str(column, upper(column))
        )
        return SnowflakeQueryCompiler(new_internal_frame)

    def str_title(self) -> "SnowflakeQueryCompiler":
        """
        Titlecase the string

        Returns
        -------
        SnowflakeQueryCompiler representing result of the string operation.
        """

        new_internal_frame = self._modin_frame.apply_snowpark_function_to_columns(
            # Capitalize the first character of each word
            lambda col: self._replace_non_str(col, initcap(col))
        )
        return SnowflakeQueryCompiler(new_internal_frame)

    def _get_regex_params(self, flags: int = 0) -> str:
        """
        Convert the flags integer into its corresponding string representation.

        Parameters
        ----------
        flags : int, default 0 (no flags)
            Flags to pass through to the re module, e.g. re.IGNORECASE.

        Returns
        -------
        String represention of the input int flags parameter.
        """
        if flags == 0:
            return "c"
        params = ""
        if flags & re.IGNORECASE:
            params = params + "i"
        else:
            params = params + "c"
        if flags & re.MULTILINE:
            params = params + "m"
        if flags & re.DOTALL:
            params = params + "s"
        return params

    def str___getitem__(self, key: Union[Scalar, slice]) -> "SnowflakeQueryCompiler":
        """
        Retrieve character(s) or substring(s) from each element in the Series or Index according to `key`.

        Parameters
        ----------
        key : scalar or slice
            Index to retrieve data from.

        Returns
        -------
        SnowflakeQueryCompiler representing result of the string operation.
        """
        if not is_scalar(key) and not isinstance(key, slice):
            # Follow pandas behavior; all values will be None.
            key = None
        if is_scalar(key):
            col = self._modin_frame.data_column_snowflake_quoted_identifiers[0]
            if key is not None and not isinstance(key, (int, str)):
                ErrorMessage.not_implemented(
                    "Snowpark pandas string indexing doesn't yet support keys of types other than int or str"
                )
            elif isinstance(
                self._modin_frame.quoted_identifier_to_snowflake_type([col]).get(col),
                MapType,
            ):
                if key is not None and not isinstance(key, str):
                    ErrorMessage.not_implemented(
                        "Snowpark pandas string indexing doesn't yet support keys "
                        "of types other than str when the data column contains dicts"
                    )
            elif isinstance(
                self._modin_frame.quoted_identifier_to_snowflake_type([col]).get(col),
                ArrayType,
            ):
                if key is not None and not isinstance(key, int):
                    ErrorMessage.not_implemented(
                        "Snowpark pandas string indexing doesn't yet support keys "
                        "of types other than int when the data column contains lists"
                    )
            else:
                if key is not None and not isinstance(key, int):
                    ErrorMessage.not_implemented(
                        "Snowpark pandas string indexing doesn't yet support keys "
                        "of types other than int when the data column contains strings"
                    )
            assert key is None or isinstance(key, (int, str))
            return self.str_get(key)
        else:
            assert isinstance(key, slice), "key is expected to be slice here"
            if key.step == 0:
                raise ValueError("slice step cannot be zero")
            return self.str_slice(key.start, key.stop, key.step)

    def str_center(self, width: int, fillchar: str = " ") -> "SnowflakeQueryCompiler":
        if not isinstance(width, int):
            raise TypeError(
                f"width must be of integer type, not {type(width).__name__}"
            )
        if not isinstance(fillchar, str):
            raise TypeError(
                f"fillchar must be a character, not {type(fillchar).__name__}"
            )
        if len(fillchar) != 1:
            raise TypeError("fillchar must be a character, not str")

        def output_col(column: SnowparkColumn) -> SnowparkColumn:
            new_col = rpad(
                lpad(
                    column,
                    greatest(
                        length(column),
                        length(column)
                        + (pandas_lit(width) - length(column) - pandas_lit(1))
                        / pandas_lit(2),
                    ),
                    pandas_lit(fillchar),
                ),
                greatest(length(column), pandas_lit(width)),
                pandas_lit(fillchar),
            )
            return self._replace_non_str(column, new_col)

        new_internal_frame = self._modin_frame.apply_snowpark_function_to_columns(
            output_col
        )
        return SnowflakeQueryCompiler(new_internal_frame)

    def str_contains(
        self,
        pat: str,
        case: bool = True,
        flags: int = 0,
        na: object = None,
        regex: bool = True,
    ) -> "SnowflakeQueryCompiler":
        """
        Test if pattern or regex is contained within a string of a Series or Index.

        Return boolean Series or Index based on whether a given pattern or regex is contained within a string of a Series or Index.

        Parameters
        ----------
        pat : str
            Character sequence or regular expression.
        case : bool, default True
            If True, case sensitive.
        flags : int, default 0 (no flags)
            Flags to pass through to the re module, e.g. re.IGNORECASE.
        na : scalar, optional
            Fill value for missing values. The default depends on dtype of the array. For object-dtype, numpy.nan is used. For StringDtype, pandas.NA is used.
        regex : bool, default True
            If True, assumes the pat is a regular expression.
            If False, treats the pat as a literal string.

        Returns
        -------
        SnowflakeQueryCompiler representing result of the string operation.
        """
        if not native_pd.isna(na) and not isinstance(na, bool):
            ErrorMessage.not_implemented(
                "Snowpark pandas doesn't support non-bool 'na' argument"
            )

        if not regex:
            pat = re.escape(pat)
            flags = 0
        pat = f"(.|\n)*({pat})(.|\n)*"
        if flags & re.IGNORECASE == 0 and not case:
            flags = flags | re.IGNORECASE
        params = self._get_regex_params(flags)

        def output_col(column: SnowparkColumn) -> SnowparkColumn:
            new_col = builtin("rlike")(column, pandas_lit(pat), pandas_lit(params))
            new_col = (
                new_col if pandas.isnull(na) else coalesce(new_col, pandas_lit(na))
            )
            return self._replace_non_str(column, new_col, replacement_value=na)

        new_internal_frame = self._modin_frame.apply_snowpark_function_to_columns(
            output_col
        )
        return SnowflakeQueryCompiler(new_internal_frame)

    def str_count(
        self, pat: str, flags: int = 0, **kwargs: Any
    ) -> "SnowflakeQueryCompiler":
        """
        Count occurrences of pattern in each string of the Series/Index.

        This function is used to count the number of times a particular regex pattern is repeated in each of the string elements of the Series.

        Parameters
        ----------
        pat : str
            Valid regular expression.
        flags : int, default 0, meaning no flags
            Flags for the re module.
        **kwargs
            For compatibility with other string methods. Not used.

        Returns
        -------
        SnowflakeQueryCompiler representing result of the string operation.
        """
        params = self._get_regex_params(flags)

        def output_col(column: SnowparkColumn) -> SnowparkColumn:
            if pat == "":
                # Special case to handle empty search pattern.
                # Snowflake's regexp_count returns 0, while pandas returns string length + 1.
                new_col = length(column) + 1
            else:
                new_col = builtin("regexp_count")(
                    column, pandas_lit(pat), 1, pandas_lit(params)
                )
            return self._replace_non_str(column, new_col)

        new_internal_frame = self._modin_frame.apply_snowpark_function_to_columns(
            output_col
        )
        return SnowflakeQueryCompiler(new_internal_frame)

    def str_get(self, i: Union[None, int, str]) -> "SnowflakeQueryCompiler":
        """
        Extract element from each component at specified position or with specified key.

        Extract element from lists, tuples, dict, or strings in each element in the Series/Index.

        Parameters
        ----------
        i : int or str
            Position or key of element to extract.

        Returns
        -------
        SnowflakeQueryCompiler representing result of the string operation.
        """
        if i is not None and not isinstance(i, (int, str)):
            ErrorMessage.not_implemented(
                "Snowpark pandas method 'Series.str.get' doesn't yet support 'i' argument of types other than int or str"
            )

        def output_col_string(
            column: SnowparkColumn, i: Union[None, int]
        ) -> SnowparkColumn:
            col_len_exp = length(column)
            if i is None:
                new_col = pandas_lit(None)
            else:
                if i < 0:
                    # Index is relative to the end boundary.
                    # If it falls before the beginning boundary, Null is returned.
                    # Note that string methods in pandas are 0-based while in Snowflake, they are 1-based.
                    new_col = iff(
                        pandas_lit(i) + col_len_exp < pandas_lit(0),
                        pandas_lit(None),
                        substring(
                            column, pandas_lit(i + 1) + col_len_exp, pandas_lit(1)
                        ),
                    )
                else:
                    assert i >= 0
                    # Index is relative to the beginning boundary.
                    # If it falls after the end boundary, Null is returned.
                    # Note that string methods in pandas are 0-based while in Snowflake, they are 1-based.
                    new_col = iff(
                        pandas_lit(i) >= col_len_exp,
                        pandas_lit(None),
                        substring(column, pandas_lit(i + 1), pandas_lit(1)),
                    )
            return self._replace_non_str(column, new_col)

        def output_col_list(
            column: SnowparkColumn, i: Union[None, int]
        ) -> SnowparkColumn:
            col_len_exp = array_size(column)
            if i is None:
                new_col = pandas_lit(None)
            else:
                if i < 0:
                    # Index is relative to the end boundary.
                    # If it falls before the beginning boundary, Null is returned.
                    # Note that string methods in pandas are 0-based while in Snowflake, they are 1-based.
                    new_col = iff(
                        pandas_lit(i) + col_len_exp < pandas_lit(0),
                        pandas_lit(None),
                        get(column, pandas_lit(i) + col_len_exp),
                    )
                else:
                    assert i >= 0
                    # Index is relative to the beginning boundary.
                    # If it falls after the end boundary, Null is returned.
                    # Note that string methods in pandas are 0-based while in Snowflake, they are 1-based.
                    new_col = iff(
                        pandas_lit(i) >= col_len_exp,
                        pandas_lit(None),
                        get(column, pandas_lit(i)),
                    )
            return new_col

        col = self._modin_frame.data_column_snowflake_quoted_identifiers[0]
        if isinstance(
            self._modin_frame.quoted_identifier_to_snowflake_type([col]).get(col),
            MapType,
        ):
            if i is not None and not isinstance(i, str):
                ErrorMessage.not_implemented(
                    "Snowpark pandas method 'Series.str.get' doesn't yet support 'i' argument "
                    "of types other than str when the data column contains dicts"
                )
            new_internal_frame = self._modin_frame.apply_snowpark_function_to_columns(
                lambda col: col[i]
            )
        elif isinstance(
            self._modin_frame.quoted_identifier_to_snowflake_type([col]).get(col),
            ArrayType,
        ):
            if i is not None and not isinstance(i, int):
                ErrorMessage.not_implemented(
                    "Snowpark pandas method 'Series.str.get' doesn't yet support 'i' argument "
                    "of types other than int when the data column contains lists"
                )
            assert i is None or isinstance(i, int)
            new_internal_frame = self._modin_frame.apply_snowpark_function_to_columns(
                lambda column: output_col_list(column, i)
            )
        else:
            if i is not None and not isinstance(i, int):
                ErrorMessage.not_implemented(
                    "Snowpark pandas method 'Series.str.get' doesn't yet support 'i' argument "
                    "of types other than int when the data column contains strings"
                )
            assert i is None or isinstance(i, int)
            new_internal_frame = self._modin_frame.apply_snowpark_function_to_columns(
                lambda column: output_col_string(column, i)
            )

        return SnowflakeQueryCompiler(new_internal_frame)

    def str_get_dummies(self, sep: str) -> None:
        ErrorMessage.method_not_implemented_error("get_dummies", "Series.str")

    def str_join(self, sep: str) -> None:
        ErrorMessage.method_not_implemented_error("join", "Series.str")

    def str_pad(
        self,
        width: int,
        side: Literal["left", "right", "both"] = "left",
        fillchar: str = " ",
    ) -> "SnowflakeQueryCompiler":
        if side == "left":
            return self.str_rjust(width, fillchar)
        elif side == "right":
            return self.str_ljust(width, fillchar)
        elif side == "both":
            return self.str_center(width, fillchar)
        else:
            raise ValueError("Invalid side")

    def str_partition(self, sep: str = " ", expand: bool = True) -> None:
        ErrorMessage.method_not_implemented_error("partition", "Series.str")

    def str_rpartition(self, sep: str = " ", expand: bool = True) -> None:
        ErrorMessage.method_not_implemented_error("rpartition", "Series.str")

    def str_len(self, **kwargs: Any) -> "SnowflakeQueryCompiler":
        """
        Compute the length of each element in the Series/Index

        Parameters
        ----------
        **kwargs
            For compatibility with other string methods. Not used.

        Returns
        -------
        SnowflakeQueryCompiler representing result of the string operation.
        """
        # TODO SNOW-1438001: Handle tuple values for Series.str.len().
        col = self._modin_frame.data_column_snowflake_quoted_identifiers[0]
        if isinstance(
            self._modin_frame.quoted_identifier_to_snowflake_type([col]).get(col),
            MapType,
        ):
            return SnowflakeQueryCompiler(
                self._modin_frame.apply_snowpark_function_to_columns(
                    lambda col: array_size(object_keys(col))
                )
            )
        elif isinstance(
            self._modin_frame.quoted_identifier_to_snowflake_type([col]).get(col),
            ArrayType,
        ):
            return SnowflakeQueryCompiler(
                self._modin_frame.apply_snowpark_function_to_columns(
                    lambda col: array_size(col)
                )
            )
        else:
            return SnowflakeQueryCompiler(
                self._modin_frame.apply_snowpark_function_to_columns(
                    lambda col: self._replace_non_str(col, length(col))
                )
            )

    def str_ljust(self, width: int, fillchar: str = " ") -> "SnowflakeQueryCompiler":
        """
        Pad right side of strings in the Series/Index.

        Equivalent to str.ljust().

        Parameters
        ----------
        width : int
            Minimum width of resulting string; additional characters will be filled with fillchar.
        fillchar : str
            Additional character for filling, default is whitespace.

        Returns
        -------
        SnowflakeQueryCompiler representing result of the string operation.
        """
        if not isinstance(width, int):
            raise TypeError(
                f"width must be of integer type, not {type(width).__name__}"
            )
        if not isinstance(fillchar, str):
            raise TypeError(
                f"fillchar must be a character, not {type(fillchar).__name__}"
            )
        if len(fillchar) != 1:
            raise TypeError("fillchar must be a character, not str")

        def output_col(column: SnowparkColumn) -> SnowparkColumn:
            new_col = rpad(
                column,
                greatest(length(column), pandas_lit(width)),
                pandas_lit(fillchar),
            )
            return self._replace_non_str(column, new_col)

        new_internal_frame = self._modin_frame.apply_snowpark_function_to_columns(
            output_col
        )
        return SnowflakeQueryCompiler(new_internal_frame)

    def str_rjust(self, width: int, fillchar: str = " ") -> "SnowflakeQueryCompiler":
        """
        Pad left side of strings in the Series/Index.

        Equivalent to str.rjust().

        Parameters
        ----------
        width : int
            Minimum width of resulting string; additional characters will be filled with fillchar.
        fillchar : str
            Additional character for filling, default is whitespace.
        Returns
        -------
        SnowflakeQueryCompiler representing result of the string operation.
        """
        if not isinstance(width, int):
            raise TypeError(
                f"width must be of integer type, not {type(width).__name__}"
            )
        if not isinstance(fillchar, str):
            raise TypeError(
                f"fillchar must be of integer type, not {type(fillchar).__name__}"
            )
        if len(fillchar) != 1:
            raise TypeError("fillchar must be a character, not str")

        def output_col(column: SnowparkColumn) -> SnowparkColumn:
            new_col = lpad(
                column,
                greatest(length(column), pandas_lit(width)),
                pandas_lit(fillchar),
            )
            return self._replace_non_str(column, new_col)

        new_internal_frame = self._modin_frame.apply_snowpark_function_to_columns(
            output_col
        )
        return SnowflakeQueryCompiler(new_internal_frame)

    def str_normalize(self, form: Literal["NFC", "NFKC", "NFD", "NFKD"]) -> None:
        ErrorMessage.method_not_implemented_error("normalize", "Series.str")

    def str_slice(
        self,
        start: Optional[int] = None,
        stop: Optional[int] = None,
        step: Optional[int] = None,
    ) -> "SnowflakeQueryCompiler":
        """
        Slice substrings from each element in the Series or Index.

        Parameters
        ----------
        start : int, optional
            Start position for slice operation.
        stop : int, optional
            Stop position for slice operation.
        step : int, optional
            Step size for slice operation.

        Returns
        -------
        SnowflakeQueryCompiler representing result of the string operation.
        """
        col = self._modin_frame.data_column_snowflake_quoted_identifiers[0]
        if (
            step is not None
            and step != 1
            and isinstance(
                self._modin_frame.quoted_identifier_to_snowflake_type([col]).get(col),
                ArrayType,
            )
        ):
            ErrorMessage.not_implemented(
                "Snowpark pandas method 'Series.str.slice' does not yet support 'step!=1' for list values"
            )

        def output_col_string(
            column: SnowparkColumn,
            start: Optional[int],
            stop: Optional[int],
            step: Optional[int],
        ) -> SnowparkColumn:
            if step is None:
                step = 1
            col_len_exp = length(column)

            # In what follows, we define the expressions needed to evaluate the correct start and stop positions for a slice.
            # In general, the start position needs to be included and the stop position needs to be excluded from the slice.
            # A negative start or stop position is relative to the end boundary of the string value.
            # Also, depending on the sign of step, we either go forward from start to stop (positive step),
            # or backwards from start to stop (negative step).
            # This means that for the stop position to be excluded in the positive step scenario, we need to
            # include the position immediately to the left of the stop position, and then stop.
            # Conversely, for the negative step scenario, we need to include the position immediately to the right
            # of the stop position, and then stop.
            # Also, the stop position is allowed to fall beyond string beginning and end boundaries.
            # However, the start position cannot fall before the beginning boundary when step is positive,
            # and similarly, it cannot fall beyond the end boundary when step is negative.
            if start is None:
                if step < 0:
                    # Start position is at the end boundary.
                    start_exp = col_len_exp
                else:
                    # Start position is at the beginning boundary.
                    start_exp = pandas_lit(1)
            elif start < 0:
                if step < 0:
                    # Start position is relative to the end boundary, and the leftmost it can
                    # get is the position immediately to the left of the beginning boundary.
                    start_exp = greatest(
                        pandas_lit(start + 1) + col_len_exp, pandas_lit(0)
                    )
                else:
                    # Start position is relative to the end boundary, and the leftmost it can
                    # get is position representing the beginning boundary.
                    start_exp = greatest(
                        pandas_lit(start + 1) + col_len_exp, pandas_lit(1)
                    )
            else:
                assert start >= 0
                if step < 0:
                    # Start position is relative to the beginning boundary, and the rightmost it can
                    # get is the position representing the end boundary.
                    start_exp = least(pandas_lit(start + 1), col_len_exp)
                else:
                    # Start position is relative to the beginning boundary, and the rightmost it can
                    # get is the position immediately to the right of the end boundary.
                    start_exp = least(
                        pandas_lit(start + 1), col_len_exp + pandas_lit(1)
                    )

            if stop is None:
                if step < 0:
                    # Stop position is immediately to the left of the beginning boundary.
                    stop_exp = pandas_lit(0)
                else:
                    # Stop position is immediately to the right of the end boundary.
                    stop_exp = col_len_exp + pandas_lit(1)
            elif stop < 0:
                # Stop position is relative to the end boundary, and the leftmost it can
                # get is the position immediately to the left of the beginning boundary.
                stop_exp = greatest(pandas_lit(stop + 1) + col_len_exp, pandas_lit(0))
            else:
                # Stop position is relative to the beginning boundary, and the rightmost it can
                # get is the position immediately to the right of the end boundary.
                stop_exp = least(pandas_lit(stop + 1), col_len_exp + pandas_lit(1))

            if step < 0:
                # When step is negative, we flip the column string value along with the start and
                # stop positions. Step can be considered positive now.
                new_col = reverse(column)
                start_exp = col_len_exp - start_exp + pandas_lit(1)
                stop_exp = col_len_exp - stop_exp + pandas_lit(1)
                step = -step
            else:
                new_col = column
            # End of evaluation for start and end positions.

            # If step is 1, then slicing is no different than getting a substring.
            # Even when step is > 1, we also start by getting the substring with all
            # the relevant characters we care about. Then we process them further below.
            new_col = substring(new_col, start_exp, stop_exp - start_exp)
            col_len_exp = stop_exp - start_exp
            if step > 1:
                # This is where the actual slicing happens using a regular expression.
                # The regex essentially identifies every consecutive substring of size (step),
                # and replaces it with its first character.
                # As preprocessing, the substring operation handles the case where the length of
                # the input string is not divisible by (step). In this case, it ensures that only
                # the first character from the residual (n % step) characters is kept. Then, when
                # processed by the regex, since this residual character won't be matched, it gets
                # output as is, which is identical to python/pandas slicing behavior.
                new_col = regexp_replace(
                    substring(
                        new_col,
                        pandas_lit(1),
                        col_len_exp - col_len_exp % pandas_lit(step) + pandas_lit(1),
                    ),
                    pandas_lit(f"((.|\n)(.|\n){{{step-1}}})"),
                    pandas_lit("\\2"),
                )
            return self._replace_non_str(column, new_col)

        def output_col_list(
            column: SnowparkColumn,
            start: Optional[int],
            stop: Optional[int],
        ) -> SnowparkColumn:
            col_len_exp = array_size(column)
            if start is None:
                start_exp = pandas_lit(0)
            else:
                start_exp = pandas_lit(start)
            if stop is None:
                stop_exp = col_len_exp + pandas_lit(1)
            else:
                stop_exp = pandas_lit(stop)
            return array_slice(column, start_exp, stop_exp)

        if isinstance(
            self._modin_frame.quoted_identifier_to_snowflake_type([col]).get(col),
            MapType,
        ):
            new_internal_frame = self._modin_frame.apply_snowpark_function_to_columns(
                # Follow pandas behavior
                lambda column: pandas_lit(np.nan).cast(FloatType()),
            )
        elif isinstance(
            self._modin_frame.quoted_identifier_to_snowflake_type([col]).get(col),
            ArrayType,
        ):
            new_internal_frame = self._modin_frame.apply_snowpark_function_to_columns(
                lambda column: output_col_list(column, start, stop)
            )
        else:
            new_internal_frame = self._modin_frame.apply_snowpark_function_to_columns(
                lambda column: output_col_string(column, start, stop, step)
            )

        return SnowflakeQueryCompiler(new_internal_frame)

    def str_slice_replace(
        self,
        start: Optional[int] = None,
        stop: Optional[int] = None,
        repl: Optional[Union[str, Callable]] = None,
    ) -> None:
        ErrorMessage.method_not_implemented_error("slice_replace", "Series.str")

    def str_split(
        self,
        pat: Optional[str] = None,
        n: int = -1,
        expand: bool = False,
        regex: Optional[bool] = None,
    ) -> "SnowflakeQueryCompiler":
        """
        Split strings around given separator/delimiter.

        Splits the string in the Series/Index from the beginning, at the specified delimiter string.

        Parameters
        ----------
        pat : str, optional
            String to split on. If not specified, split on whitespace.
        n : int, default -1 (all)
            Limit number of splits in output. None, 0 and -1 will be interpreted as return all splits.
        expand : bool, default False (Not implemented yet, should be set to False)
            Expand the split strings into separate columns.
            - If True, return DataFrame/MultiIndex expanding dimensionality.
            - If False, return Series/Index, containing lists of strings.
        regex : bool, default None (Not implemented yet, should be set to False or None)
            Determines if the passed-in pattern is a regular expression:
            - If True, assumes the passed-in pattern is a regular expression
            - If False or None, treats the pattern as a literal string.

        Returns
        -------
        SnowflakeQueryCompiler representing result of string operation.
        """
        if pat is not None and not isinstance(pat, str):
            ErrorMessage.not_implemented(
                "Snowpark pandas doesn't support non-str 'pat' argument"
            )
        if regex:
            ErrorMessage.not_implemented(
                "Snowpark pandas doesn't support 'regex' argument"
            )
        if pandas.isnull(regex):
            regex = False
        if not pat and pat is not None:
            raise ValueError("split() requires a non-empty pattern match.")

        if n is None:
            n = -1
        elif not isinstance(n, (int, float)):
            ErrorMessage.not_implemented(
                "Snowpark pandas doesn't support non-numeric 'n' argument"
            )

        def output_col(
            column: SnowparkColumn, pat: Optional[str], n: int
        ) -> SnowparkColumn:
            if pandas.isnull(pat):
                # When pat is null, it means we need to split on whitespace.
                # For this purpose, we replace all sequences of whitespace characters with a single space.
                # And we also trim whitespace from both ends of the string column.
                new_pat = " "
                whitespace_chars = " \t\r\n\f"
                regex_pat = r"\s+"
                regex_pat_as_prefix = r"\s+.*"
                new_col = builtin("regexp_replace")(
                    builtin("trim")(column, pandas_lit(whitespace_chars)),
                    pandas_lit(regex_pat),
                    pandas_lit(" "),
                )

                n_for_split_idx = iff(
                    builtin("regexp_like")(column, pandas_lit(regex_pat_as_prefix)),
                    pandas_lit(n + 1),
                    pandas_lit(n),
                )
            else:
                new_pat = str(pat)
                regex_pat = re.escape(str(pat))
                new_col = column
                n_for_split_idx = pandas_lit(n)

            if np.isnan(n):
                # Follow pandas behavior
                return pandas_lit(np.nan)
            elif n < -1 and not pandas.isnull(pat) and len(str(pat)) > 1:
                # Follow pandas behavior, which based on our experiments, leaves the input column as is
                # whenever the above condition is satisfied.
                new_col = iff(
                    column.is_null(), pandas_lit(None), array_construct(column)
                )
            elif n <= 0:
                # If all possible splits are requested, we just use SQL's split function.
                new_col = builtin("split")(new_col, pandas_lit(new_pat))
            else:
                # If a maximum number of splits is required, then we need to add logic to check
                # if the delimiter (or pat) occurs enough times to satisfy the desired number of splits.
                # If so, then SQL's split can be used.
                # Otherwise (i.e., there are more delimiter occurrences than required for the split),
                # we need to divide the string column into two parts - left and right:
                # - The left part should have the requested number of delimiters - 1,
                #   such that it can be split into n parts.
                # - The right part will constitute the remaining (n+1st) part. In other words,
                #   it will not be split and will remain intact irrespective of the number of
                #   delimiter occurrences it has.
                split_idx = builtin("regexp_instr")(
                    column, pandas_lit(regex_pat), 1, n_for_split_idx, 1
                )
                new_col = iff(
                    builtin("array_size")(
                        builtin("split")(new_col, pandas_lit(new_pat))
                    )
                    <= pandas_lit(n + 1),
                    builtin("split")(new_col, pandas_lit(new_pat)),
                    builtin("array_append")(
                        builtin("array_slice")(
                            builtin("split")(new_col, pandas_lit(new_pat)),
                            pandas_lit(0),
                            pandas_lit(n),
                        ),
                        builtin("substr")(
                            column,
                            split_idx,
                        ),
                    ),
                )
            if pandas.isnull(pat):
                new_col = iff(
                    builtin("regexp_like")(column, pandas_lit(r"\s*")),
                    pandas_lit([]),
                    new_col,
                )
            return self._replace_non_str(column, new_col)

        def output_cols(
            column: SnowparkColumn, pat: Optional[str], n: int, max_splits: int
        ) -> list[SnowparkColumn]:
            """
            Returns the list of columns that the input column will be split into.
            This is only used when expand=True.
            Args:
                column : SnowparkColumn
                    Input column
                pat : str
                    String to split on
                n : int
                    Limit on the number of output splits
                max_splits : int
                    Maximum number of achievable splits across all values in the input column.
                    This is needed to be able to pad rows with fewer splits than desired with nulls.
            """
            col = output_col(column, pat, n)
            final_splits = 0

            if np.isnan(n):
                # Follow pandas behavior
                final_splits = 1
            elif n <= 0:
                final_splits = max_splits
            else:
                final_splits = min(n + 1, max_splits)

            if n < -1 and not pandas.isnull(pat) and len(str(pat)) > 1:
                # Follow pandas behavior, which based on our experiments, leaves the input column as is
                # whenever the above condition is satisfied.
                final_splits = 1

            return [
                iff(
                    array_size(col) > pandas_lit(i),
                    get(col, pandas_lit(i)),
                    pandas_lit(None),
                )
                for i in range(final_splits)
            ]

        def get_max_splits() -> int:
            """
            Returns the maximum number of splits achievable
            across all values stored in the input column.
            """
            splits_as_list_frame = self.str_split(
                pat=pat,
                n=-1,
                expand=False,
                regex=regex,
            )._modin_frame

            split_counts_frame = splits_as_list_frame.append_column(
                "split_counts",
                array_size(
                    col(
                        splits_as_list_frame.data_column_snowflake_quoted_identifiers[0]
                    )
                ),
            )

            max_count_rows = split_counts_frame.ordered_dataframe.agg(
                max_(
                    col(split_counts_frame.data_column_snowflake_quoted_identifiers[-1])
                ).as_("max_count")
            ).collect()

            return max_count_rows[0][0]

        if expand:
            cols = output_cols(
                col(self._modin_frame.data_column_snowflake_quoted_identifiers[0]),
                pat,
                n,
                get_max_splits(),
            )
            new_internal_frame = self._modin_frame.project_columns(
                list(range(len(cols))),
                cols,
            )
        else:
            new_internal_frame = self._modin_frame.apply_snowpark_function_to_columns(
                lambda col_name: output_col(col_name, pat, n)
            )

        return SnowflakeQueryCompiler(new_internal_frame)

    def str_rsplit(
        self, pat: Optional[str] = None, *, n: int = -1, expand: bool = False
    ) -> None:
        ErrorMessage.method_not_implemented_error("rsplit", "Series.str")

    def str_replace(
        self,
        pat: str,
        repl: Union[str, Callable],
        n: int = -1,
        case: Optional[bool] = None,
        flags: int = 0,
        regex: bool = True,
    ) -> "SnowflakeQueryCompiler":
        """
        Replace each occurrence of pattern/regex in the Series/Index.

        Equivalent to str.replace() or re.sub(), depending on the regex value.

        Parameters
        ----------
        pat : str
            String can be a character sequence or regular expression.
        repl : str or callable
            Replacement string or a callable. The callable is passed the regex match object and must return a replacement string to be used. See re.sub().
        n : int, default -1 (all)
            Number of replacements to make from start.
        case : bool, default None
            Determines if replace is case sensitive:
            - If True, case sensitive (the default if pat is a string)
            - Set to False for case insensitive
            - Cannot be set if pat is a compiled regex.
        flags : int, default 0 (no flags)
            Regex module flags, e.g. re.IGNORECASE. Cannot be set if pat is a compiled regex.
        regex : bool, default False
            Determines if the passed-in pattern is a regular expression:
            - If True, assumes the passed-in pattern is a regular expression.
            - If False, treats the pattern as a literal string
            - Cannot be set to False if pat is a compiled regex or repl is a callable.

        Returns
        -------
        SnowflakeQueryCompiler representing result of string operation.
        """
        if pat is None or not isinstance(pat, str):
            ErrorMessage.not_implemented(
                "Snowpark pandas doesn't support non-str 'pat' argument"
            )

        if callable(repl) or not isinstance(repl, str):
            ErrorMessage.not_implemented(
                "Snowpark pandas doesn't support non-str 'repl' argument"
            )

        if pandas.isnull(n):
            n = -1
        elif not isinstance(n, (int, float)) or n == 0:
            ErrorMessage.not_implemented(
                "Snowpark pandas doesn't support non-numeric or zero-valued 'n' argument"
            )

        if pandas.isnull(case):
            case = True
        if flags & re.IGNORECASE > 0:
            case = False
        if flags & re.IGNORECASE == 0 and not case:
            flags = flags | re.IGNORECASE

        def output_col(
            column: SnowparkColumn, pat: str, n: int, flags: int
        ) -> SnowparkColumn:
            if regex or (case is not None and not case) or n > 0:
                # Here we handle the cases where SQL's regexp_replace rather than SQL's replace
                # needs to be used.
                if not regex:
                    pat = re.escape(pat)
                params = self._get_regex_params(flags)
                if n < 0:
                    # Replace all occurrences.
                    new_col = builtin("regexp_replace")(column, pat, repl, 1, 0, params)
                elif n == 1:
                    # Replace first occurrence.
                    new_col = builtin("regexp_replace")(column, pat, repl, 1, 1, params)
                else:
                    # Replace first n occurences through these steps:
                    # (1) Find index of nth occurence (if present).
                    # (2) Use found index as a splitting point between a left and a right part of the string column.
                    # (3) Replace all occurrences in the left part and leave right part unchanged.
                    # (4) Concat left and right parts.
                    split_idx = iff(
                        builtin("regexp_instr")(column, pat, 1, 1, 1, params) == 0,
                        0,
                        iff(
                            builtin("regexp_instr")(column, pat, 1, n, 1, params) == 0,
                            builtin("len")(column) + 1,
                            builtin("regexp_instr")(column, pat, 1, n, 1, params),
                        )
                        - 1,
                    )
                    new_col = builtin("concat")(
                        builtin("regexp_replace")(
                            builtin("left")(column, split_idx),
                            pat,
                            repl,
                            1,
                            0,
                            params,
                        ),
                        builtin("right")(column, builtin("len")(column) - split_idx),
                    )
            else:
                # Replace all occurrences using SQL's replace.
                new_col = builtin("replace")(column, pat, repl)
            return self._replace_non_str(column, new_col)

        new_internal_frame = self._modin_frame.apply_snowpark_function_to_columns(
            lambda column: output_col(column, pat, n, flags)
        )
        return SnowflakeQueryCompiler(new_internal_frame)

    def str_repeat(self, repeats: int) -> None:
        ErrorMessage.method_not_implemented_error("repeat", "Series.str")

    def str_removeprefix(self, prefix: str) -> None:
        ErrorMessage.method_not_implemented_error("removeprefix", "Series.str")

    def str_removesuffix(self, prefix: str) -> None:
        ErrorMessage.method_not_implemented_error("removesuffix", "Series.str")

    def _str_strip_variant(
        self, sp_func: Callable, pd_func_name: str, to_strip: Union[str, None] = None
    ) -> "SnowflakeQueryCompiler":
        """
        Remove leading and/or trailing characters depending on sp_func.

        Strip whitespaces (including newlines) or a set of specified characters from each string in the Series/Index from left and/or right sides depending on sp_func. Replaces any non-strings in Series with NaNs. Equivalent to str.strip(), str.lstrip(), or str.rstrip() depending on sp_func.

        Parameters
        ----------
        sp_func: Callable
            Snopwark function to use - trim, ltrim, or rtrim.
        pd_func_name: str
            Name of pandas string function - strip, lstrip, or rstrip.
        to_strip : str or None, default None
            Specifying the set of characters to be removed. All combinations of this set of characters will be stripped. If None then whitespaces are removed.

        Returns
        -------
        SnowflakeQueryCompiler representing result of the string operation.
        """
        if not pandas.isnull(to_strip) and not isinstance(to_strip, str):
            ErrorMessage.not_implemented(
                f"Snowpark pandas Series.str.{pd_func_name} does not yet support non-str 'to_strip' argument"
            )
        if to_strip is None:
            to_strip = "\t\n\r\f "

        def output_col(column: SnowparkColumn) -> SnowparkColumn:
            new_col = sp_func(column, pandas_lit(to_strip))
            return self._replace_non_str(column, new_col)

        new_internal_frame = self._modin_frame.apply_snowpark_function_to_columns(
            output_col
        )
        return SnowflakeQueryCompiler(new_internal_frame)

    @snowpark_pandas_type_immutable_check
    def str_strip(self, to_strip: Union[str, None] = None) -> "SnowflakeQueryCompiler":
        """
        Remove leading and trailing characters.

        Strip whitespaces (including newlines) or a set of specified characters from each string in the Series/Index from left and right sides. Replaces any non-strings in Series with NaNs. Equivalent to str.strip().

        Parameters
        ----------
        to_strip : str or None, default None
            Specifying the set of characters to be removed. All combinations of this set of characters will be stripped. If None then whitespaces are removed.

        Returns
        -------
        SnowflakeQueryCompiler representing result of the string operation.
        """
        return self._str_strip_variant(
            sp_func=trim, pd_func_name="strip", to_strip=to_strip
        )

    @snowpark_pandas_type_immutable_check
    def str_lstrip(self, to_strip: Union[str, None] = None) -> "SnowflakeQueryCompiler":
        """
        Remove leading characters.

        Strip whitespaces (including newlines) or a set of specified characters from each string in the Series/Index from left side. Replaces any non-strings in Series with NaNs. Equivalent to str.lstrip().

        Parameters
        ----------
        to_strip : str or None, default None
            Specifying the set of characters to be removed. All combinations of this set of characters will be stripped. If None then whitespaces are removed.

        Returns
        -------
        SnowflakeQueryCompiler representing result of the string operation.
        """
        return self._str_strip_variant(
            sp_func=ltrim, pd_func_name="lstrip", to_strip=to_strip
        )

    @snowpark_pandas_type_immutable_check
    def str_rstrip(self, to_strip: Union[str, None] = None) -> "SnowflakeQueryCompiler":
        """
        Remove trailing characters.

        Strip whitespaces (including newlines) or a set of specified characters from each string in the Series/Index from right side. Replaces any non-strings in Series with NaNs. Equivalent to str.rstrip().

        Parameters
        ----------
        to_strip : str or None, default None
            Specifying the set of characters to be removed. All combinations of this set of characters will be stripped. If None then whitespaces are removed.

        Returns
        -------
        SnowflakeQueryCompiler representing result of the string operation.
        """
        return self._str_strip_variant(
            sp_func=rtrim, pd_func_name="rstrip", to_strip=to_strip
        )

    def str_swapcase(self) -> None:
        ErrorMessage.method_not_implemented_error("swapcase", "Series.str")

    @snowpark_pandas_type_immutable_check
    def str_translate(self, table: dict) -> "SnowflakeQueryCompiler":
        """
        Map all characters in the string through the given mapping table.

        Equivalent to standard :meth:`str.translate`.

        Parameters
        ----------
        table : dict
            Table is a mapping of Unicode ordinals to Unicode ordinals, strings, or
            None. Unmapped characters are left untouched.
            Characters mapped to None are deleted. :meth:`str.maketrans` is a
            helper function for making translation tables.

        Returns
        -------
        SnowflakeQueryCompiler representing results of the string operation.
        """
        # Snowflake SQL TRANSLATE:
        #   TRANSLATE(<subject>, <sourceAlphabet>, <targetAlphabet>)
        # Characters in the <sourceAlphabet> string are mapped to the corresponding entry in <targetAlphabet>.
        # If <sourceAlphabet> is longer than <targetAlphabet>, then the trailing characters of <sourceAlphabet>
        # are removed from the input string.
        #
        # Because TRANSLATE only supports 1-to-1 character mappings, any entries with multi-character
        # values must be handled by REPLACE instead. 1-character keys are always invalid.
        single_char_pairs = {}
        none_keys = set()
        for key, value in table.items():
            # Treat integers as unicode codepoints
            if isinstance(key, int):
                key = chr(key)
            if isinstance(value, int):
                value = chr(value)
            if len(key) != 1:
                # Mimic error from str.maketrans
                raise ValueError(
                    f"Invalid mapping key '{key}'. String keys in translate table must be of length 1."
                )
            if value is not None and len(value) > 1:
                raise NotImplementedError(
                    f"Invalid mapping value '{value}' for key '{key}'. Snowpark pandas currently only "
                    "supports unicode ordinals or 1-codepoint strings as values in str.translate mappings. "
                    "Consider using Series.str.replace to replace multiple characters."
                )
            if value is None or len(value) == 0:
                none_keys.add(key)
            else:
                single_char_pairs[key] = value
        source_alphabet = "".join(single_char_pairs.keys()) + "".join(none_keys)
        target_alphabet = "".join(single_char_pairs.values())
        return SnowflakeQueryCompiler(
            self._modin_frame.apply_snowpark_function_to_columns(
                lambda column: translate(
                    column,
                    pandas_lit(source_alphabet),
                    pandas_lit(target_alphabet),
                )
            )
        )

    def str_wrap(self, width: int, **kwargs: Any) -> None:
        ErrorMessage.method_not_implemented_error("wrap", "Series.str")

    def str_zfill(self, width: int) -> None:
        ErrorMessage.method_not_implemented_error("zfill", "Series.str")

    def qcut(
        self,
        q: Union[int, ListLike],
        retbins: bool,
        duplicates: Union[Literal["raise", "drop"]],
        precision: int = 3,
    ) -> "SnowflakeQueryCompiler":
        """
        Computes for self (which is assumed to be representing a Series) into which bins data falls.
        Args:
            q: integer or list of floating point quantiles (increasing)
            retbins: return bins as well (not supported yet)
            duplicates: When constructing bins from quantiles, duplicate bins may exist. If 'raise' abort execution and report to user a ValueError, if 'drop' remove duplicate bins and continue.
            precision: Bins are constructed as left-open intervals of the form (a, b]. Depending on the precision specified (default: 3), to distinguish whether an element falls into a bin b_1 or b_2, the value is decreased by an epsilon = 10**(-precision).
        Returns:
            SnowflakeQueryCompiler representing a Series with indices to the bins.
        """

        if retbins is True:
            # TODO: SNOW-1225562, support retbins=True.
            ErrorMessage.not_implemented("no support for returning bins yet.")

        # There are two cases to consider:
        # 1. q is an integer, which means divide the data into q equiwidth bins.
        # 2. q is a list of floats representing quantiles (must be [0, 1], checked in frontend) from which
        #    bins are constructed.

        # If q is an integer, construct the correct quantiles first.
        if isinstance(q, int):
            # taken from pandas.
            q = list(np.linspace(0, 1, q + 1))

        # Construct bins from quantiles.
        # First step is to transform the quantiles given as a list of float values in q to values according to the data.
        # We add a new ARRAY column 'quantiles' with quantile values.
        data_column = col(self._modin_frame.data_column_snowflake_quoted_identifiers[0])
        frame = self._modin_frame.append_column(
            "quantiles",
            array_construct(
                *[
                    builtin("percentile_cont")(pandas_lit(quantile))
                    .within_group(data_column)
                    .over()
                    .cast(DoubleType())
                    for quantile in q
                ]
            ),
        )
        quantile_column_snowlake_identifier = (
            frame.data_column_snowflake_quoted_identifiers[-1]
        )
        # There are two behaviors here:
        # - If duplicates = 'raise', check if there are duplicates and raise an error.
        # - If drop, ignore and continue with distinct quantile values.

        # Note: This eager query can be avoided for case duplicates = 'drop' by using a
        # combination of higher-order function FILTER and ARRAY_POSITION. But FILTER
        # is not yet supported in snowpark.
        # TODO SNOW-1375054: perform this eager query only when dupliates = 'raise'
        # For duplicates = 'drop' calcuate qcut lazily using ARRAY_POSITION(FILTER(...))

        # Try to fetch 2 rows from the dataframe. We use number of rows returned here
        # to determine if the frame has single element or is empty.
        first_two_rows = (
            frame.ordered_dataframe.select(quantile_column_snowlake_identifier)
            .limit(2)
            .collect()
        )
        # Array is returned as serialied json. Create list from serialized string.
        quantiles = json.loads(first_two_rows[0][0])

        if duplicates == "raise":
            # Check if there are duplicates, and raise if so.
            # If not, proceed and assume quantiles to be duplicate free.
            n_unique = len(set(quantiles))
            if n_unique != len(q):
                # if self has a single element or is empty, duplicates are ok - even for 'raise'.
                if len(first_two_rows) > 1:
                    # throw Pandas compatible error message
                    raise ValueError(
                        f"Bin edges must be unique: {quantiles}.\nYou can drop duplicate edges by setting the 'duplicates' kwarg"
                    )

        # other duplicates case ('drop') is handled here.
        unique_quantiles = list(dict.fromkeys(quantiles))

        # There will be 0, ..., len(unique_quantiles) - 1 cuts, result will be thus in this range.
        # We can find for values the cut they belong to by comparing against quantiled values.
        case_expr: Optional[CaseExpr] = None
        for index, quantile in enumerate(unique_quantiles):
            bin = max(index - 1, 0)
            cond = data_column <= quantile
            case_expr = (
                when(cond, bin) if case_expr is None else case_expr.when(cond, bin)
            )
        case_expr = (
            case_expr.otherwise(None) if case_expr is not None else pandas_lit(None)
        )

        frame = frame.append_column("qcut_bin", case_expr)
        new_data_identifier = frame.data_column_snowflake_quoted_identifiers[-1]

        new_frame = InternalFrame.create(
            ordered_dataframe=frame.ordered_dataframe,
            data_column_pandas_labels=self._modin_frame.data_column_pandas_labels,
            data_column_pandas_index_names=self._modin_frame.data_column_index_names,
            data_column_snowflake_quoted_identifiers=[new_data_identifier],
            index_column_pandas_labels=self._modin_frame.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=self._modin_frame.index_column_snowflake_quoted_identifiers,
            data_column_types=self._modin_frame.cached_data_column_snowpark_pandas_types,
            index_column_types=self._modin_frame.cached_index_column_snowpark_pandas_types,
        )

        return SnowflakeQueryCompiler(new_frame)

    def _groupby_head_tail(
        self,
        n: int,
        op_type: Literal["head", "tail"],
        by: Any,
        level: Optional[IndexLabel],
        dropna: bool,
    ) -> "SnowflakeQueryCompiler":
        """
        Select the first or last n rows/entries in a group.

        Helper function for DataFrameGroupBy.head and DataFrameGroupBy.tail. Since both use similar logic, it is
        combined in this method. In this helper:
        - if n == 0, an empty frame is returned.
        - if op_type="head" and n > 0, select the first n entries in the group.
        - if op_type="head" and n < 0, exclude the last n entries in the group.
        - if op_type="tail" and n > 0, select the last n entries in the group.
        - if op_type="tail" and n < 0, exclude the first n entries in the group.

        Args:
            n: number of entries to select. For head and tail, the rows selected varies based on the sign of n.
            op_type: Whether a head or tail operation needs to be performed.
            by: Used to determine the groups for the groupby.
            level: If the axis is a MultiIndex (hierarchical), group by a particular level or levels.
                Do not specify both by and level.
            dropna: Whether the rows with NA group keys need to be dropped.

        Returns:
            A SnowflakeQueryCompiler object representing a DataFrame.
        """
        original_frame = self._modin_frame

        assert op_type in ["head", "tail"], "op_type must be head or tail."

        if n == 0:
            # None of the rows should be selected, an empty DataFrame must be returned.
            return SnowflakeQueryCompiler(original_frame.filter(pandas_lit(False)))

        # STEP 1: Extract the column(s) used to group the data by.
        query_compiler, by_list = resample_and_extract_groupby_column_pandas_labels(
            self,
            by,
            level,
            skip_resample=True,
        )
        original_frame = query_compiler._modin_frame
        ordered_dataframe = original_frame.ordered_dataframe
        by_snowflake_quoted_identifiers_list = [
            entry[0]
            for entry in original_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                by_list
            )
        ]
        # Copy of the snowflake_quoted_identifiers of the column(s) used to group the data by.
        partition_list = by_snowflake_quoted_identifiers_list.copy()

        # STEP 2: Create a select list containing the index columns, data columns, and columns added for
        # generating the filtering condition (groupby row number column, groupby count column).
        select_list = []
        # Record the new snowflake_quoted_identifiers that the grouping columns use
        # this is used in STEP 3 to determine which rows to drop when the group keys are NA values.
        new_groupby_sf_identifiers = []

        # Add required index columns to the select list.
        # Recording index column identifiers for creating a new internal frame in STEP 4.
        # Generate identifiers for every column beforehand to handle duplicate column labels.
        index_column_aliases = ordered_dataframe.generate_snowflake_quoted_identifiers(
            pandas_labels=original_frame.index_column_pandas_labels
        )
        index_column_snowflake_quoted_identifiers = []
        for col_ident, col_alias in zip(
            original_frame.index_column_snowflake_quoted_identifiers,
            index_column_aliases,
        ):
            # An alias is required for all columns selected from the ordered dataframe.
            select_list.append(col(col_ident).alias(col_alias))
            index_column_snowflake_quoted_identifiers.append(col_alias)
            if col_ident in by_snowflake_quoted_identifiers_list:
                # The grouping identifiers when `level` is specified come from the index columns.
                new_groupby_sf_identifiers.append(col_alias)

        # Add required data columns to the select list.
        # Recording data column identifiers for creating a new internal frame in STEP 4.
        # Generate identifiers for every column beforehand to handle duplicate column labels.
        data_column_aliases = ordered_dataframe.generate_snowflake_quoted_identifiers(
            pandas_labels=original_frame.data_column_pandas_labels
        )
        data_column_snowflake_quoted_identifiers = []
        for col_ident, col_alias in zip(
            original_frame.data_column_snowflake_quoted_identifiers,
            data_column_aliases,
        ):
            # An alias is required for all columns selected from the ordered dataframe.
            select_list.append(col(col_ident).alias(col_alias))
            data_column_snowflake_quoted_identifiers.append(col_alias)
            if col_ident in by_snowflake_quoted_identifiers_list:
                # The grouping identifiers when `by` is specified come from the data columns.
                new_groupby_sf_identifiers.append(col_alias)

        # Create a column to record the row numbers in every group. This helps us identify each row.
        grouped_row_num_label = "grouped_row_num_label"
        grouped_row_num_alias = ordered_dataframe.generate_snowflake_quoted_identifiers(
            pandas_labels=[grouped_row_num_label]
        )[0]
        select_list.append(
            row_number()
            .over(
                Window.partition_by(partition_list).order_by(
                    ordered_dataframe._ordering_snowpark_columns()
                )
            )
            .alias(grouped_row_num_alias)
        )

        # Creating a column to find the largest row number in every group. This helps with selecting
        # the last n entries in a frame, and when we need to perform an exclusive operation (n < 0).
        count_label = "grouped_count_row_label"
        count_alias = ordered_dataframe.generate_snowflake_quoted_identifiers(
            pandas_labels=[count_label]
        )[0]
        select_list.append(
            count("*").over(Window.partition_by(partition_list)).alias(count_alias)
        )

        # STEP 3: Create an ordered_dataframe that is grouped by the specified "groupby column(s)".
        # Then create the filtering conditions using the groupby row number and count columns.
        grouped_ordered_dataframe = ordered_dataframe.select(select_list)

        grouped_row_num_id, grouped_count_id = -2, -1
        # Get the column which represents the row numbers in every group. This is the penultimate column.
        grouped_row_num_col = col(
            grouped_ordered_dataframe.projected_column_snowflake_quoted_identifiers[
                grouped_row_num_id
            ]
        )
        # Get the column which represents the largest row number in every group. This is the last column.
        grouped_count_col = col(
            grouped_ordered_dataframe.projected_column_snowflake_quoted_identifiers[
                grouped_count_id
            ]
        )

        # Creating the filter conditions. Row number starts at 1, not 0.
        if n > 0:  # select operations
            if op_type == "head":
                # Select first n rows.
                filter_cond = grouped_row_num_col <= pandas_lit(n)
            else:  # op_type == "tail"
                # Select last n rows.
                filter_cond = grouped_row_num_col > (grouped_count_col - pandas_lit(n))
        else:  # n < 0, exclusive operations
            if op_type == "head":
                # Exclude the last n rows in a group.
                filter_cond = grouped_row_num_col <= (grouped_count_col + pandas_lit(n))
            else:  # op_type == "tail"
                # Exclude the first n rows in a group.
                filter_cond = grouped_row_num_col > pandas_lit(n * -1)

        # If dropna=True, need to drop the rows where an NA value is present in any of the grouping columns, i.e.,
        # the grouping column has a NA group key.
        if dropna and len(new_groupby_sf_identifiers) > 0:
            dropna_cond = functools.reduce(
                lambda combined_col, col: combined_col | col,
                map(
                    lambda by_snowflake_quoted_identifier: col(
                        by_snowflake_quoted_identifier
                    ).is_null(),
                    new_groupby_sf_identifiers,
                ),
            )
            # Add the dropna condition to the filter condition.
            filter_cond = filter_cond & ~dropna_cond

        # STEP 4: Filter the grouped ordered_frame and create a new internal_frame and qc from it.
        filtered_ordered_dataframe = grouped_ordered_dataframe.filter(filter_cond)
        new_modin_frame = InternalFrame.create(
            ordered_dataframe=filtered_ordered_dataframe,
            data_column_pandas_labels=original_frame.data_column_pandas_labels,
            data_column_pandas_index_names=[None],  # operation removes names
            data_column_snowflake_quoted_identifiers=data_column_snowflake_quoted_identifiers,
            index_column_pandas_labels=original_frame.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=index_column_snowflake_quoted_identifiers,
            data_column_types=original_frame.cached_data_column_snowpark_pandas_types,
            index_column_types=original_frame.cached_index_column_snowpark_pandas_types,
        )

        return SnowflakeQueryCompiler(new_modin_frame)

    def cut(
        self,
        bins: Union[int, Sequence[Scalar], pandas.IntervalIndex],
        right: bool = True,
        labels: Union[ListLike, bool, None] = None,
        precision: int = 3,
        include_lowest: bool = False,
        duplicates: str = "raise",
    ) -> tuple[Sequence[Scalar], "SnowflakeQueryCompiler"]:
        """
        Compute result of pd.cut for self, which is assumed to be a Series.

        Args:
            bins: see cut
            right: see cut
            labels: see cut
            precision: see cut
            include_lowest: see cut
            duplicates: see cut

        Returns:
            tuple of (adjusted) bins, and a QC representing the result as Series of the operation.
        """

        # Retrieve min/max from self. If empty, abort with ValueError as in pandas.
        min, max, row_count = (
            self.agg(["min", "max", "count"], axis=0, args=(), kwargs={})
            .to_pandas()
            .squeeze(axis=1)
        )

        if row_count == 0:
            raise ValueError("Cannot cut empty array")

        bins = preprocess_bins_for_cut(min, max, bins, right, include_lowest, precision)

        # If duplicates is set to 'raise', check for duplicates.
        # If 'drop', remove duplicate edges here
        if duplicates == "raise":
            if len(set(bins)) < len(bins):
                raise ValueError(
                    f"Bin edges must be unique: {repr(bins)}.\nYou can drop duplicate edges by setting the 'duplicates' kwarg"
                )
        else:
            bins = sorted(list(set(bins)))

        qc_bins = SnowflakeQueryCompiler.from_pandas(pandas.DataFrame(bins))

        bin_indices_frame = compute_bin_indices(
            self._modin_frame, qc_bins._modin_frame, len(bins), right
        )

        # If labels=None, instead of returning indices return intervals.
        # We do not support intervals in Snowpark Pandas yet, an error is produced in TypeMapper.to_snowflake.
        if labels is None:
            # labels will be based on indices
            labels = [
                pandas.Interval(bins[i], bins[i + 1]) for i in range(len(bins) - 1)
            ]

        if labels is False:
            # Directly return result, no adjustment necessary to convert bin indices -> bin labels.
            return bins, SnowflakeQueryCompiler(bin_indices_frame)

        assert isinstance(labels, list)

        # Note: In Snowpark pandas API, we do not support Interval.
        # This means that labels=None will produce an error of the form
        # TypeError: Can not infer schema for type: <class 'pandas._libs.interval.Interval'>
        # originating in TypeMapper.to_snowflake.
        # This error is surfaced as is, to support labels=None, the logic here does not need to get changed
        # but first-class support for pd.Interval needs to get added in the ORM.

        # Raise pandas compatible error.
        if len(set(labels)) != len(labels):
            raise ValueError(
                "labels must be unique if ordered=True; pass ordered=False "
                "for duplicate labels"
            )

        # Raise pandas-compatible error.
        if len(labels) + 1 != len(bins):
            raise ValueError(
                "Bin labels must be one fewer than the number of bin edges"
            )

        labels_frame = SnowflakeQueryCompiler.from_pandas(
            pandas.DataFrame(labels)
        )._modin_frame.ensure_row_position_column()

        # Join with labels and return result from there, i.e. replace value of i with labels[i].
        join_ret = join_utils.join(
            bin_indices_frame,
            labels_frame,
            how="left",
            left_on=[bin_indices_frame.data_column_snowflake_quoted_identifiers[0]],
            right_on=[labels_frame.row_position_snowflake_quoted_identifier],
        )

        ret_frame = join_ret.result_frame.project_columns(
            pandas_labels=[None],
            column_objects=[
                col(
                    join_ret.result_column_mapper.right_quoted_identifiers_map[
                        labels_frame.data_column_snowflake_quoted_identifiers[0]
                    ]
                )
            ],
        )

        return bins, SnowflakeQueryCompiler(ret_frame)

    def str_casefold(self) -> None:
        ErrorMessage.method_not_implemented_error("casefold", "Series.str")

    def dt_to_period(self, freq: Optional[str] = None) -> None:
        """
        Convert underlying data to the period at a particular frequency.

        Parameters
        ----------
        freq : str, optional

        Returns
        -------
        BaseQueryCompiler
            New QueryCompiler containing period data.
        """
        ErrorMessage.not_implemented(
            "Snowpark pandas doesn't yet support the method 'Series.dt.to_period'"
        )

    def dt_to_pydatetime(self) -> None:
        """
        Convert underlying data to array of python native ``datetime``.

        Returns
        -------
        BaseQueryCompiler
            New QueryCompiler containing 1D array of ``datetime`` objects.
        """
        ErrorMessage.not_implemented(
            "Snowpark pandas doesn't yet support the method 'Series.dt.to_pydatetime'"
        )

    # FIXME: there are no references to this method, we should either remove it
    # or add a call reference at the DataFrame level (Modin issue #3103).
    def dt_to_pytimedelta(self) -> None:
        """
        Convert underlying data to array of python native ``datetime.timedelta``.

        Returns
        -------
        BaseQueryCompiler
            New QueryCompiler containing 1D array of ``datetime.timedelta``.
        """
        ErrorMessage.not_implemented(
            "Snowpark pandas doesn't yet support the method 'Series.dt.to_pytimedelta'"
        )

    def dt_to_timestamp(self) -> None:
        """
        Convert underlying data to the timestamp

        Returns
        -------
        BaseQueryCompiler
            New QueryCompiler containing timestamp data.
        """
        ErrorMessage.not_implemented(
            "Snowpark pandas doesn't yet support the method 'Series.dt.to_timestamp'"
        )

    def dt_tz_localize(
        self,
        tz: Union[str, tzinfo],
        ambiguous: str = "raise",
        nonexistent: str = "raise",
        include_index: bool = False,
    ) -> "SnowflakeQueryCompiler":
        """
        Localize tz-naive to tz-aware.
        Args:
            tz : str, pytz.timezone, optional
            ambiguous : {"raise", "inner", "NaT"} or bool mask, default: "raise"
            nonexistent : {"raise", "shift_forward", "shift_backward, "NaT"} or pandas.timedelta, default: "raise"
            include_index: Whether to include the index columns in the operation.

        Returns:
            BaseQueryCompiler
                New QueryCompiler containing values with localized time zone.
        """
        dtype = self.index_dtypes[0] if include_index else self.dtypes[0]
        if not include_index:
            method_name = "Series.dt.tz_localize"
        else:
            assert is_datetime64_any_dtype(dtype), "column must be datetime"
            method_name = "DatetimeIndex.tz_localize"

        if not isinstance(ambiguous, str) or ambiguous != "raise":
            ErrorMessage.parameter_not_implemented_error("ambiguous", method_name)
        if not isinstance(nonexistent, str) or nonexistent != "raise":
            ErrorMessage.parameter_not_implemented_error("nonexistent", method_name)
        if isinstance(tz, str) and tz not in pytz.all_timezones:
            ErrorMessage.not_implemented(
                f"Snowpark pandas method '{method_name}' doesn't support 'tz={tz}'"
            )

        return SnowflakeQueryCompiler(
            self._modin_frame.apply_snowpark_function_to_columns(
                lambda column: tz_localize_column(column, tz),
                include_index=include_index,
            )
        )

    def dt_tz_convert(
        self,
        tz: Union[str, tzinfo],
        include_index: bool = False,
    ) -> "SnowflakeQueryCompiler":
        """
        Convert time-series data to the specified time zone.

        Args:
            tz : str, pytz.timezone
            include_index: Whether to include the index columns in the operation.

        Returns:
            A new QueryCompiler containing values with converted time zone.
        """
        if not include_index:
            method_name = "Series.dt.tz_convert"
        else:
            method_name = "DatetimeIndex.tz_convert"
        if isinstance(tz, str) and tz not in pytz.all_timezones:
            ErrorMessage.not_implemented(
                f"Snowpark pandas method '{method_name}' doesn't support 'tz={tz}'"
            )

        return SnowflakeQueryCompiler(
            self._modin_frame.apply_snowpark_function_to_columns(
                lambda column: tz_convert_column(column, tz),
                include_index=include_index,
            )
        )

    def dt_ceil(
        self,
        freq: Frequency,
        ambiguous: str = "raise",
        nonexistent: str = "raise",
        include_index: bool = False,
    ) -> "SnowflakeQueryCompiler":
        """
        Args:
            freq: The frequency level to ceil the index to.
            ambiguous: 'infer', bool-ndarray, 'NaT', default 'raise'
                Only relevant for DatetimeIndex:
                - 'infer' will attempt to infer fall dst-transition hours based on order
                - bool-ndarray where True signifies a DST time, False designates a non-DST time (note that this flag is only applicable for ambiguous times)
                - 'NaT' will return NaT where there are ambiguous times
                - 'raise' will raise an AmbiguousTimeError if there are ambiguous times.
            nonexistent: 'shift_forward', 'shift_backward', 'NaT', timedelta, default 'raise'
                A nonexistent time does not exist in a particular timezone where clocks moved forward due to DST.
                - 'shift_forward' will shift the nonexistent time forward to the closest existing time
                - 'shift_backward' will shift the nonexistent time backward to the closest existing time
                - 'NaT' will return NaT where there are nonexistent times
                - timedelta objects will shift nonexistent times by the timedelta
                - 'raise' will raise an NonExistentTimeError if there are nonexistent times.
            include_index: Whether to include the index columns in the operation.
        Returns:
            A new QueryCompiler with ceil values.

        """
        dtype = self.index_dtypes[0] if include_index else self.dtypes[0]
        if not include_index:
            method_name = "Series.dt.ceil"
        elif is_datetime64_any_dtype(dtype):
            method_name = "DatetimeIndex.ceil"
        elif is_timedelta64_dtype(dtype):
            method_name = "TimedeltaIndex.ceil"
        else:
            raise AssertionError(
                "column must be datetime or timedelta"
            )  # pragma: no cover

        if not isinstance(ambiguous, str) or ambiguous != "raise":
            ErrorMessage.parameter_not_implemented_error("ambiguous", method_name)
        if not isinstance(nonexistent, str) or nonexistent != "raise":
            ErrorMessage.parameter_not_implemented_error("nonexistent", method_name)

        if is_datetime64_any_dtype(dtype):
            slice_length, slice_unit = rule_to_snowflake_width_and_slice_unit(freq)

            if slice_unit not in SUPPORTED_DT_FLOOR_CEIL_FREQS:
                ErrorMessage.parameter_not_implemented_error(
                    f"freq='{freq}'", method_name
                )
            return_type = None

            def ceil_func(column: SnowparkColumn) -> SnowparkColumn:
                floor_column = builtin("time_slice")(
                    column, slice_length, slice_unit, "START"
                )
                ceil_column = builtin("time_slice")(
                    column, slice_length, slice_unit, "END"
                )
                return iff(column.equal_null(floor_column), column, ceil_column)

        else:  # timedelta type
            nanos = timedelta_freq_to_nanos(freq)
            if nanos == 0:
                # no conversion needed.
                return self
            return_type = TimedeltaType()

            def ceil_func(column: SnowparkColumn) -> SnowparkColumn:
                return iff(
                    column % nanos == 0, column, column + nanos - (column % nanos)
                )

        return SnowflakeQueryCompiler(
            self._modin_frame.apply_snowpark_function_to_columns(
                ceil_func,
                include_index=include_index,
                return_type=return_type,
            )
        )

    def dt_round(
        self,
        freq: Frequency,
        ambiguous: str = "raise",
        nonexistent: str = "raise",
        include_index: bool = False,
    ) -> "SnowflakeQueryCompiler":
        """
        Args:
            freq: The frequency level to round the index to.
            ambiguous: 'infer', bool-ndarray, 'NaT', default 'raise'
                Only relevant for DatetimeIndex:
                - 'infer' will attempt to infer fall dst-transition hours based on order
                - bool-ndarray where True signifies a DST time, False designates a non-DST time (note that this flag is only applicable for ambiguous times)
                - 'NaT' will return NaT where there are ambiguous times
                - 'raise' will raise an AmbiguousTimeError if there are ambiguous times.
            nonexistent: 'shift_forward', 'shift_backward', 'NaT', timedelta, default 'raise'
                A nonexistent time does not exist in a particular timezone where clocks moved forward due to DST.
                - 'shift_forward' will shift the nonexistent time forward to the closest existing time
                - 'shift_backward' will shift the nonexistent time backward to the closest existing time
                - 'NaT' will return NaT where there are nonexistent times
                - timedelta objects will shift nonexistent times by the timedelta
                - 'raise' will raise an NonExistentTimeError if there are nonexistent times.
            include_index: Whether to include the index columns in the operation.
        Returns:
            A new QueryCompiler with round values.

        """
        dtype = self.index_dtypes[0] if include_index else self.dtypes[0]
        if not include_index:
            method_name = "Series.dt.round"
        elif is_datetime64_any_dtype(dtype):
            method_name = "DatetimeIndex.round"
        elif is_timedelta64_dtype(dtype):
            method_name = "TimedeltaIndex.round"
        else:
            raise AssertionError(
                "column must be datetime or timedelta"
            )  # pragma: no cover

        if not isinstance(ambiguous, str) or ambiguous != "raise":
            ErrorMessage.parameter_not_implemented_error("ambiguous", method_name)
        if not isinstance(nonexistent, str) or nonexistent != "raise":
            ErrorMessage.parameter_not_implemented_error("nonexistent", method_name)

        if is_datetime64_any_dtype(dtype):
            slice_length, slice_unit = rule_to_snowflake_width_and_slice_unit(freq)

            if (
                slice_unit not in SUPPORTED_DT_FLOOR_CEIL_FREQS
                or slice_unit == "second"
            ):
                ErrorMessage.parameter_not_implemented_error(
                    f"freq={freq}", method_name
                )

            # We need to implement the algorithm for rounding half to even whenever
            # the date value is at half point of the slice:
            # https://en.wikipedia.org/wiki/Rounding#Rounding_half_to_even

            # First, we need to calculate the length of half a slice.
            # This is straightforward if the length is already even.
            # If not, we then need to first downlevel the freq to a
            # lower granularity to ensure that it is even.
            # TODO: Explore if it's possible to replace it with bround.

            def down_level_freq(slice_length: int, slice_unit: str) -> tuple[int, str]:
                if slice_unit == "minute":
                    slice_length *= 60
                    slice_unit = "second"
                elif slice_unit == "hour":
                    slice_length *= 60
                    slice_unit = "minute"
                elif slice_unit == "day":
                    slice_length *= 24
                    slice_unit = "hour"
                else:
                    # We already check valid 'freq' above. We should never reach here.
                    assert slice_unit in ("minute", "hour", "day")
                return slice_length, slice_unit

            if slice_length % 2 == 1:
                slice_length, slice_unit = down_level_freq(slice_length, slice_unit)
            half_slice_length = int(slice_length / 2)
            return_type = None

            def slice_length_when_unit_is_second(
                slice_length: int, slice_unit: str
            ) -> int:
                while slice_unit != "second":
                    slice_length, slice_unit = down_level_freq(slice_length, slice_unit)
                return slice_length

            def round_func(column: SnowparkColumn) -> SnowparkColumn:
                # Second, we determine whether floor represents an even number of slices.
                # To do so, we must divide the number of epoch seconds in it over the number
                # of epoch seconds in one slice. This way, we can get the number of slices.

                floor_column = builtin("time_slice")(
                    column, slice_length, slice_unit, "START"
                )
                ceil_column = builtin("time_slice")(
                    column, slice_length, slice_unit, "END"
                )

                floor_epoch_seconds_column = builtin("extract")(
                    "epoch_second", floor_column
                )
                floor_num_slices_column = cast(
                    floor_epoch_seconds_column
                    / pandas_lit(
                        slice_length_when_unit_is_second(slice_length, slice_unit)
                    ),
                    IntegerType(),
                )

                # Now that we know the number of slices, we can check if they are even or odd.
                floor_is_even = (floor_num_slices_column % pandas_lit(2)).equal_null(
                    pandas_lit(0)
                )

                # Accordingly, we can decide if the round column should be the floor or ceil
                # of the slice.
                round_column_if_half_point = iff(
                    floor_is_even, floor_column, ceil_column
                )

                # In case the date value is not at half point of the slice, then we shift it
                # by half a slice, and take the floor from there.
                base_plus_half_slice_column = dateadd(
                    slice_unit, pandas_lit(half_slice_length), column
                )
                round_column_if_not_half_point = builtin("time_slice")(
                    base_plus_half_slice_column, slice_length, slice_unit, "START"
                )

                # The final expression for the round column.
                return iff(
                    base_plus_half_slice_column.equal_null(ceil_column),
                    round_column_if_half_point,
                    round_column_if_not_half_point,
                )

        else:  # timedelta type
            nanos = timedelta_freq_to_nanos(freq)
            if nanos == 0:
                # no conversion needed.
                return self
            return_type = TimedeltaType()

            def round_func(column: SnowparkColumn) -> SnowparkColumn:
                return bround(column / nanos, 0) * nanos

        return SnowflakeQueryCompiler(
            self._modin_frame.apply_snowpark_function_to_columns(
                round_func,
                include_index=include_index,
                return_type=return_type,
            )
        )

    def dt_floor(
        self,
        freq: Frequency,
        ambiguous: str = "raise",
        nonexistent: str = "raise",
        include_index: bool = False,
    ) -> "SnowflakeQueryCompiler":
        """
        Args:
            freq: The frequency level to floor the index to.
            ambiguous: 'infer', bool-ndarray, 'NaT', default 'raise'
                Only relevant for DatetimeIndex:
                - 'infer' will attempt to infer fall dst-transition hours based on order
                - bool-ndarray where True signifies a DST time, False designates a non-DST time (note that this flag is only applicable for ambiguous times)
                - 'NaT' will return NaT where there are ambiguous times
                - 'raise' will raise an AmbiguousTimeError if there are ambiguous times.
            nonexistent: 'shift_forward', 'shift_backward', 'NaT', timedelta, default 'raise'
                A nonexistent time does not exist in a particular timezone where clocks moved forward due to DST.
                - 'shift_forward' will shift the nonexistent time forward to the closest existing time
                - 'shift_backward' will shift the nonexistent time backward to the closest existing time
                - 'NaT' will return NaT where there are nonexistent times
                - timedelta objects will shift nonexistent times by the timedelta
                - 'raise' will raise an NonExistentTimeError if there are nonexistent times.
            include_index: Whether to include the index columns in the operation.
        Returns:
            A new QueryCompiler with floor values.
        """
        # This method should support both datetime and timedelta types.
        dtype = self.index_dtypes[0] if include_index else self.dtypes[0]
        if not include_index:
            method_name = "Series.dt.floor"
        elif is_datetime64_any_dtype(dtype):
            method_name = "DatetimeIndex.floor"
        elif is_timedelta64_dtype(dtype):
            method_name = "TimedeltaIndex.floor"
        else:
            raise AssertionError(
                "column must be datetime or timedelta"
            )  # pragma: no cover

        if not isinstance(ambiguous, str) or ambiguous != "raise":
            ErrorMessage.parameter_not_implemented_error("ambiguous", method_name)
        if not isinstance(nonexistent, str) or nonexistent != "raise":
            ErrorMessage.parameter_not_implemented_error("nonexistent", method_name)

        if is_datetime64_any_dtype(dtype):
            slice_length, slice_unit = rule_to_snowflake_width_and_slice_unit(freq)

            if slice_unit not in SUPPORTED_DT_FLOOR_CEIL_FREQS:
                ErrorMessage.parameter_not_implemented_error(
                    f"freq='{freq}'", method_name
                )
            return_type = None

            def floor_func(column: SnowparkColumn) -> SnowparkColumn:
                return builtin("time_slice")(column, slice_length, slice_unit)

        else:  # timedelta type
            nanos = timedelta_freq_to_nanos(freq)
            if nanos == 0:
                # no conversion needed.
                return self
            return_type = TimedeltaType()

            def floor_func(column: SnowparkColumn) -> SnowparkColumn:
                return column - (column % nanos)

        return SnowflakeQueryCompiler(
            self._modin_frame.apply_snowpark_function_to_columns(
                floor_func,
                include_index=include_index,
                return_type=return_type,
            ),
        )

    def dt_normalize(self, include_index: bool = False) -> "SnowflakeQueryCompiler":
        """
        Set the time component of each date-time value to midnight.

        Args:
            include_index: Whether to include the index columns in the operation.

        Returns
        -------
        BaseQueryCompiler
            New QueryCompiler containing date-time values with midnight time.
        """

        def normalize_column(column: SnowparkColumn) -> SnowparkColumn:
            return builtin("date_trunc")("d", column)

        return SnowflakeQueryCompiler(
            self._modin_frame.apply_snowpark_function_to_columns(
                normalize_column,
                include_index=include_index,
            )
        )

    def dt_month_name(
        self, locale: Optional[str] = None, include_index: bool = False
    ) -> "SnowflakeQueryCompiler":
        """
        Args:
            locale: Locale determining the language in which to return the month name.
            include_index: Whether to include the index columns in the operation.

        Returns:
            New QueryCompiler containing month name.
        """
        if locale is not None:
            class_name = "DatetimeIndex" if include_index else "Series.dt"
            ErrorMessage.parameter_not_implemented_error(
                "locale", f"{class_name}.month_name"
            )

        # The following generates a mapping list of the form:
        # [1, "January", 2, "February", ..., 12, "December"]
        mapping_list = [
            int(i / 2) if i % 2 == 0 else calendar.month_name[int(i / 2)]
            for i in range(2, 26)
        ]

        def month_name_func(column: SnowparkColumn) -> SnowparkColumn:
            return builtin("decode")(builtin("extract")("month", column), *mapping_list)

        return SnowflakeQueryCompiler(
            self._modin_frame.apply_snowpark_function_to_columns(
                month_name_func,
                include_index=include_index,
            )
        )

    def dt_day_name(
        self, locale: Optional[str] = None, include_index: bool = False
    ) -> "SnowflakeQueryCompiler":
        """
        Args:
            locale: Locale determining the language in which to return the month name.
            include_index: Whether to include the index columns in the operation.

        Returns:
            New QueryCompiler containing day name.
        """
        if locale is not None:
            class_name = "DatetimeIndex" if include_index else "Series.dt"
            ErrorMessage.parameter_not_implemented_error(
                "locale", f"{class_name}.day_name"
            )

        # The following generates a mapping list of the form:
        # [1, "Monday", 2, "Tuesday", ..., 7, "Sunday"]
        mapping_list = [
            int(i / 2) + 1 if i % 2 == 0 else calendar.day_name[int(i / 2)]
            for i in range(0, 14)
        ]

        def day_name_func(column: SnowparkColumn) -> SnowparkColumn:
            return builtin("decode")(builtin("dayofweekiso")(column), *mapping_list)

        return SnowflakeQueryCompiler(
            self._modin_frame.apply_snowpark_function_to_columns(
                day_name_func,
                include_index=include_index,
            )
        )

    def dt_total_seconds(self, include_index: bool = False) -> "SnowflakeQueryCompiler":
        """
        Return total duration of each element expressed in seconds.
        Args:
            include_index: Whether to include the index columns in the operation.
        Returns:
            New QueryCompiler containing total seconds.
        """
        # This method is only applicable to timedelta types.
        dtype = self.index_dtypes[0] if include_index else self.dtypes[0]
        if not is_timedelta64_dtype(dtype):
            raise AttributeError(
                "'DatetimeProperties' object has no attribute 'total_seconds'"
            )
        return SnowflakeQueryCompiler(
            self._modin_frame.apply_snowpark_function_to_columns(
                # Cast the column to decimal of scale 9 to ensure no precision loss.
                lambda x: x.cast(DecimalType(scale=9)) / 1_000_000_000,
                include_index=include_index,
            )
        )

    def dt_strftime(self, date_format: str) -> "SnowflakeQueryCompiler":
        """
        Format underlying date-time data using specified format.

        Args:
            date_format: str

        Returns:
            New QueryCompiler containing formatted date-time values.
        """

        def strftime_func(column: SnowparkColumn) -> SnowparkColumn:
            directive_to_function_map: dict[str, Callable] = {
                "d": (
                    # Day of the month as a zero-padded decimal number
                    lambda column: lpad(
                        dayofmonth(column), pandas_lit(2), pandas_lit("0")
                    )
                ),
                "m": (
                    # Month as a zero-padded decimal number
                    lambda column: lpad(month(column), pandas_lit(2), pandas_lit("0"))
                ),
                "Y": (
                    # Year with century as a decimal number
                    lambda column: lpad(year(column), pandas_lit(4), pandas_lit("0"))
                ),
                "H": (
                    # Hour (24-hour clock) as a zero-padded decimal number
                    lambda column: lpad(hour(column), pandas_lit(2), pandas_lit("0"))
                ),
                "M": (
                    # Minute as a zero-padded decimal number
                    lambda column: lpad(minute(column), pandas_lit(2), pandas_lit("0"))
                ),
                "S": (
                    # Second as a zero-padded decimal number
                    lambda column: lpad(second(column), pandas_lit(2), pandas_lit("0"))
                ),
                "f": (
                    # Microsecond as a decimal number, zero-padded to 6 digits
                    lambda column: lpad(
                        floor(date_part("ns", column) / 1000),
                        pandas_lit(6),
                        pandas_lit("0"),
                    )
                ),
                "j": (
                    # Day of the year as a zero-padded decimal number
                    lambda column: lpad(
                        dayofyear(column), pandas_lit(3), pandas_lit("0")
                    )
                ),
                "X": (
                    # Locale’s appropriate time representation
                    lambda column: trunc(to_time(column), pandas_lit("second"))
                ),
                "%": (
                    # A literal '%' character
                    lambda column: pandas_lit("%")
                ),
            }

            parts = re.split("%.", date_format)
            directive_first = False
            if parts[0] == "":
                parts = parts[1:]
                directive_first = True
            if parts[-1] == "":
                parts = parts[:-1]
            directives = re.findall("%.", date_format)
            cols = []
            for i in range(min(len(parts), len(directives))):
                directive_function = directive_to_function_map.get(directives[i][1:])
                if not directive_function:
                    raise ErrorMessage.not_implemented(
                        f"Snowpark pandas 'Series.dt.strftime' method does not yet support the directive '%{directives[i][1:]}'"
                    )

                if directive_first:
                    cols.append(directive_function(column))
                    cols.append(pandas_lit(parts[i]))
                else:
                    cols.append(pandas_lit(parts[i]))
                    cols.append(directive_function(column))

            if len(parts) > len(directives):
                cols.append(pandas_lit(parts[-1]))
            if len(parts) < len(directives):
                directive_function = directive_to_function_map.get(directives[-1][1:])
                if not directive_function:
                    raise ErrorMessage.not_implemented(
                        f"Snowpark pandas 'Series.dt.strftime' method does not yet support the directive '%{directives[-1][1:]}'"
                    )
                cols.append(directive_function(column))

            if len(cols) == 1:
                return iff(column.is_null(), pandas_lit(None), cols[0])
            else:
                return iff(column.is_null(), pandas_lit(None), concat(*cols))

        return SnowflakeQueryCompiler(
            self._modin_frame.apply_snowpark_function_to_columns(
                strftime_func,
                include_index=False,
            )
        )

    def topn(
        self, n: int, columns: IndexLabel, keep: str, ascending: bool
    ) -> "SnowflakeQueryCompiler":
        """
        Return the top 'n' rows ordered by 'columns'..

        Args:
            n: Number of rows to return.
            columns: Column label(s) to order by.
            keep: {'first', 'last', 'all'}
              Where there are duplicate values:
              first : prioritize the first occurrence(s)
              last : prioritize the last occurrence(s)
              all : do not drop any duplicates, even it means selecting more than n items.
            ascending: {True, False}
              If True return smallest n values otherwise return largest n values.

        Returns:
            SnowflakeQueryCompiler
        """
        if keep not in ("first", "last", "all"):
            raise ValueError('keep must be either "first", "last" or "all"')
        if keep == "all":
            method_name = "nsmallest" if ascending else "nlargest"
            # TODO SNOW-1483214: Add support for keep='all'
            ErrorMessage.not_implemented(
                f"Snowpark pandas method '{method_name}' doesn't yet support parameter keep='all'"
            )
        # Special case handling for unnamed series. 'columns' passed from frontend layer
        # will be None, replace it with MODIN_UNNAMED_SERIES_LABEL.
        if self._modin_frame.is_unnamed_series():
            columns = MODIN_UNNAMED_SERIES_LABEL

        if not is_list_like(columns):
            columns = [columns]

        # Native pandas returns empty dataframe if n is negative. Set it to zero to
        # provide same behavior.
        n = max(0, n)
        # Native pandas returns empty dataframe if 'columns' is a empty array. Set n
        # to zero to provide same behavior.
        if len(columns) == 0:
            n = 0

        internal_frame = self._modin_frame
        # Map pandas labels to snowflake identifiers.
        matched_identifiers = (
            internal_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                columns, include_index=False
            )
        )
        ordering_columns = []
        for label, identifiers in zip(columns, matched_identifiers):
            if len(identifiers) == 0:
                raise KeyError(label)
            if len(identifiers) > 1:
                raise ValueError(f"The column label '{label}' is not unique.")
            ordering_columns.append(
                OrderingColumn(identifiers[0], ascending, na_last=True)
            )

        # Append existing ordering column to handle duplicates.
        for ordering_column in internal_frame.ordering_columns:
            # reverse the sort order if keep is 'last'
            if keep == "last":
                ordering_column = OrderingColumn(
                    ordering_column.snowflake_quoted_identifier,
                    not ordering_column.ascending,
                    ordering_column.na_last,
                )
            ordering_columns.append(ordering_column)

        ordered_frame = internal_frame.ordered_dataframe.sort(ordering_columns).limit(n)
        return SnowflakeQueryCompiler(
            InternalFrame.create(
                ordered_dataframe=ordered_frame,
                data_column_pandas_labels=internal_frame.data_column_pandas_labels,
                data_column_snowflake_quoted_identifiers=internal_frame.data_column_snowflake_quoted_identifiers,
                data_column_pandas_index_names=internal_frame.data_column_pandas_index_names,
                index_column_pandas_labels=internal_frame.index_column_pandas_labels,
                index_column_snowflake_quoted_identifiers=internal_frame.index_column_snowflake_quoted_identifiers,
                data_column_types=internal_frame.cached_data_column_snowpark_pandas_types,
                index_column_types=internal_frame.cached_index_column_snowpark_pandas_types,
            )
        )

    def nlargest(
        self, n: int, columns: IndexLabel, keep: str
    ) -> "SnowflakeQueryCompiler":
        """
        Return the first 'n' rows ordered by 'columns' in descending order.

        Args:
            n: Number of rows to return.
            columns: Column label(s) to order by.
            keep: {'first', 'last', 'all'}
              Where there are duplicate values:
              first : prioritize the first occurrence(s)
              last : prioritize the last occurrence(s)
              all : do not drop any duplicates, even it means selecting more than n items.

        Returns:
            SnowflakeQueryCompiler
        """
        return self.topn(n, columns, keep, ascending=False)

    def nsmallest(
        self, n: int, columns: IndexLabel, keep: str
    ) -> "SnowflakeQueryCompiler":
        """
        Return the first 'n' rows ordered by 'columns' in ascending order.

        Args:
            n: Number of rows to return.
            columns: Column label(s) to order by.
            keep: {'first', 'last', 'all'}
              Where there are duplicate values:
              first : prioritize the first occurrence(s)
              last : prioritize the last occurrence(s)
              all : do not drop any duplicates, even it means selecting more than n items.

        Returns:
            SnowflakeQueryCompiler
        """
        return self.topn(n, columns, keep, ascending=True)

    def pct_change(
        self,
        periods: int = 1,
        fill_method: Literal["backfill", "bfill", "pad", "ffill", None] = "pad",
        limit: Optional[int] = None,
        freq: Optional[Union[pd.DateOffset, timedelta, str]] = None,
        axis: Axis = 0,
        by_labels: Optional[List[str]] = None,
        drop_by_labels: bool = False,
        **kwargs: Any,
    ) -> "SnowflakeQueryCompiler":
        """
        Fractional change between the current and a prior element.

        Computes the fractional change from the immediately previous row by default.
        This is useful in comparing the fraction of change in a time series of elements.

        Parameters
        ----------
        periods : int, default 1
            Periods to shift for forming percent change.

        fill_method : {'backfill', 'bfill', 'pad', 'ffill'}, default 'pad'
            How to handle NAs before computing percent changes.

        limit : int, optional
            The number of consecutive NAs to fill before stopping.

            Snowpark pandas does not yet support this parameter.

        freq : DateOffset, timedelta, or str, optional
            Increment to use from time series API (e.g. ‘ME’ or BDay()).

            Snowpark pandas does not yet support this parameter.

        axis : Axis, default 0
            This is not part of the documented `pct_change` API, but pandas forwards kwargs like this
            to `shift`. To avoid unnecessary JOIN operations, we cannot compositionally use `QueryCompiler.shift`,
            and instead have to validate the axis argument here.

        by_labels : List[str], optional
            A list of pandas labels used during a groupby. This is not part of the pandas `pct_change`
            API, and used only as an internal helper for `groupby_pct_change`. Only valid if axis=0.

            We must pass labels instead of quoted identifiers in case we perform a fillna,
            which would change quoted identifiers in the filled frame.

        drop_by_labels : bool, default False
            Whether the `by` labels need to be dropped from the result. This is not part of the pandas `pct_change`
            API, and used only as an internal helper for `groupby_pct_change`. Only valid if axis=0.
        """
        # `periods` is validated by the frontend
        if limit is not None:
            ErrorMessage.not_implemented(
                "Snowpark pandas DataFrame/Series.pct_change does not yet support the 'limit' parameter"
            )
        if freq is not None:
            ErrorMessage.not_implemented(
                "Snowpark pandas DataFrame/Series.pct_change does not yet support the 'freq' parameter"
            )
        frame = self._modin_frame
        if fill_method is not None:
            frame = self.fillna(
                self_is_series=False, method=fill_method, axis=axis
            )._modin_frame

        if axis == 0:
            by_identifiers = None
            if by_labels:
                by_identifiers = [
                    snowflake_quoted_identifier[0]
                    for snowflake_quoted_identifier in frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                        by_labels, include_index=True, include_data=True
                    )
                ]

            result_qc = SnowflakeQueryCompiler(
                frame.update_snowflake_quoted_identifiers_with_expressions(
                    {
                        quoted_identifier:
                        # If periods=0, we don't need to do any window computation
                        iff(
                            is_null(col(quoted_identifier)),
                            pandas_lit(None, FloatType()),
                            pandas_lit(0),
                        )
                        if periods == 0
                        else (
                            col(quoted_identifier)
                            / lag(quoted_identifier, offset=periods).over(
                                (
                                    # If this is a groupby, add a PARTITION BY clause
                                    Window
                                    if by_identifiers is None
                                    else Window.partition_by(by_identifiers)
                                ).orderBy(
                                    col(frame.row_position_snowflake_quoted_identifier)
                                )
                            )
                            - 1
                        )
                        for quoted_identifier in frame.data_column_snowflake_quoted_identifiers
                        # If this is a groupby, don't include the grouping column
                        if not by_identifiers or quoted_identifier not in by_identifiers
                    }
                ).frame
            )
            if drop_by_labels:
                result_qc = result_qc.drop(columns=by_labels)
            return result_qc
        else:
            quoted_identifiers = frame.data_column_snowflake_quoted_identifiers
            return SnowflakeQueryCompiler(
                frame.update_snowflake_quoted_identifiers_with_expressions(
                    {
                        quoted_identifier:
                        # If periods=0, we don't need to do any computation
                        iff(
                            is_null(col(quoted_identifier)),
                            pandas_lit(None, FloatType()),
                            pandas_lit(0),
                        )
                        if periods == 0
                        else (
                            # If periods>0, the first few columns will be NULL
                            # If periods<0, the last few columns will be NULL
                            pandas_lit(None, FloatType())
                            if i - periods < 0 or i - periods >= len(quoted_identifiers)
                            # For the remaining columns, if periods=n, we compare column i to column i+n
                            else col(quoted_identifier)
                            / col(quoted_identifiers[i - periods])
                            - 1
                        )
                        for i, quoted_identifier in enumerate(quoted_identifiers)
                    }
                ).frame
            )

    def index_equals(self, other: "SnowflakeQueryCompiler") -> bool:
        """
        Compare self index against other index.
        The things that are being compared are:
        * The elements inside the Index object.
        * The order of the elements inside the Index object.

        Args:
            other: Snowflake query compiler to compare against.

        Returns:
            True if self index is equal to other index, False otherwise.
        """
        # Join on row position columns to compare order of data.
        self_frame = self._modin_frame.ensure_row_position_column()
        other_frame = other._modin_frame.ensure_row_position_column()
        join_result = join_utils.join(
            self_frame,
            other_frame,
            left_on=[self_frame.row_position_snowflake_quoted_identifier],
            right_on=[other_frame.row_position_snowflake_quoted_identifier],
            how="outer",
            inherit_join_index=InheritJoinIndex.FROM_BOTH,
        )

        agg_exprs = {
            builtin("booland_agg")(col(left_id).equal_null(col(right_id))).as_(left_id)
            for left_id, right_id in zip(
                join_result.result_column_mapper.map_left_quoted_identifiers(
                    self_frame.index_column_snowflake_quoted_identifiers
                ),
                join_result.result_column_mapper.map_right_quoted_identifiers(
                    other_frame.index_column_snowflake_quoted_identifiers
                ),
            )
        }

        try:
            rows = join_result.result_frame.ordered_dataframe.agg(agg_exprs).collect()
        except SnowparkSQLException:
            return False
        # In case of empty table/dataframe booland_agg returns None. Add special case
        # handling for that.
        return all(x is None for x in rows[0]) or all(rows[0])

    def equals(
        self, other: "SnowflakeQueryCompiler", include_index: bool = False
    ) -> "SnowflakeQueryCompiler":
        """
        Compare self against other, element-wise (binary operator equal_null).
        Notes:
        1. Assumes both query compilers have equal row and column index labels.
          df.equals(other) should also compare row and column index labels but
          that is already handled in frontend layer. This method only needs to compare
          data column values.
        2. Nulls/NaNs at same location are considered equal. This differs from
          _binary_op("eq") where Nulls/NaNs at same location are considered different.
        3. Columns with different types are not considered equal even if values are
          same. For example 1 != 1.0. In native pandas this check is very strict
          where integer varients are also not considered euqal i.e 1 (int8) != 1 (int16)
          But in Snowpark pandas we consider them equal because we don't have one to one
          mapping of these integer types. We still consider float and integer types
          different same as native pandas.

        Args:
            other: Snowflake query compiler to compare against.

        Returns:
            Query compiler with boolean values.
        """
        self_frame = self._modin_frame
        other_frame = other._modin_frame

        # Index column names and data column labels might be different. Data column
        # labels must be equal in value (already checked by frontend) but they could
        # still differ in type.
        # Match labels by assigning from self to other.
        other_frame = InternalFrame.create(
            ordered_dataframe=other_frame.ordered_dataframe,
            data_column_pandas_labels=self_frame.data_column_pandas_labels,
            data_column_snowflake_quoted_identifiers=other_frame.data_column_snowflake_quoted_identifiers,
            index_column_pandas_labels=self_frame.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=other_frame.index_column_snowflake_quoted_identifiers,
            data_column_pandas_index_names=self_frame.data_column_pandas_index_names,
            data_column_types=other_frame.cached_data_column_snowpark_pandas_types,
            index_column_types=other_frame.cached_index_column_snowpark_pandas_types,
        )

        # Align (join) both dataframes on index.
        align_result = join_utils.align_on_index(self._modin_frame, other._modin_frame)

        left_right_pairs = prepare_binop_pairs_between_dataframe_and_dataframe(
            align_result, self_frame.data_column_pandas_labels, self_frame, other_frame
        )

        replace_mapping = {
            p.identifier: BinaryOp.create(
                "equal_null", p.lhs, p.lhs_datatype, p.rhs, p.rhs_datatype
            )
            .compute()
            .snowpark_column
            for p in left_right_pairs
        }

        # Create new frame by replacing columns with equal_null expressions.
        updated_result = align_result.result_frame.update_snowflake_quoted_identifiers_with_expressions(
            replace_mapping
        )
        updated_data_identifiers = [
            updated_result.old_id_to_new_id_mappings[p.identifier]
            for p in left_right_pairs
        ]
        updated_data_identifiers_types = [
            updated_result.frame.snowflake_quoted_identifier_to_snowpark_pandas_type[id]
            for id in updated_data_identifiers
        ]
        new_frame = updated_result.frame
        result_frame = InternalFrame.create(
            ordered_dataframe=new_frame.ordered_dataframe,
            data_column_pandas_labels=self_frame.data_column_pandas_labels,
            data_column_pandas_index_names=new_frame.data_column_pandas_index_names,
            data_column_snowflake_quoted_identifiers=updated_data_identifiers,
            index_column_pandas_labels=new_frame.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=new_frame.index_column_snowflake_quoted_identifiers,
            data_column_types=updated_data_identifiers_types,
            index_column_types=new_frame.cached_index_column_snowpark_pandas_types,
        )

        return SnowflakeQueryCompiler(result_frame)

    def stack(
        self,
        level: Union[int, str, list] = -1,
        dropna: bool = True,
        sort: bool = True,
    ) -> "SnowflakeQueryCompiler":
        """
        Stack the prescribed level(s) from columns to index.

        Return a reshaped DataFrame or Series having a multi-level index with one
        or more new inner-most levels compared to the current DataFrame. The new inner-most
        levels are created by pivoting the columns of the current dataframe:
            - if the columns have a single level, the output is a Series.
            - if the columns have multiple levels, the new index level(s) is (are)
              taken from the prescribed level(s) and the output is a DataFrame.

        Parameters
        ----------
        level : int, str, list, default -1
            Level(s) to stack from the column axis onto the index axis,
            defined as one index or label, or a list of indices or labels.

        dropna : bool, default True
            Whether to drop rows in the resulting Frame/Series with missing values. Stacking a
            column level onto the index axis can create combinations of index and column values
            that are missing from the original dataframe.

        sort : bool, default True
            Whether to sort the levels of the resulting MultiIndex.
        """
        # stack() may create a column that includes values from multiple input
        # columns. Tracking types in that case is not simple, so we don't
        # handle the client-side timedelta type as an input.
        self._raise_not_implemented_error_for_timedelta()

        if level != -1:
            ErrorMessage.not_implemented(
                "Snowpark pandas doesn't yet support 'level != -1' in stack API",
            )
        if self._modin_frame.is_multiindex(axis=1):
            ErrorMessage.not_implemented(
                "Snowpark pandas doesn't support multiindex columns in stack API"
            )

        qc = self._stack_helper(operation=StackOperation.STACK)

        if dropna:
            return qc.dropna(axis=0, how="any", thresh=None)
        else:
            return qc

    def unstack(
        self,
        level: Union[int, str, list] = -1,
        fill_value: Optional[Union[int, str, dict]] = None,
        sort: bool = True,
        is_series_input: bool = False,
    ) -> "SnowflakeQueryCompiler":
        """
        Pivot a level of the (necessarily hierarchical) index labels.

        Returns a DataFrame having a new level of column labels whose
        inner-most level consists of the pivoted index labels.

        If the index is not a MultiIndex, the output will be a Series
        (the analogue of stack when the columns are not a MultiIndex).

        Parameters
        ----------
        level : int, str, list, default -1
            Level(s) of index to unstack, can pass level name.

        fillna : int, str, dict, optional
            Replace NaN with this value if the unstack produces missing values.

        sort : bool, default True
            Sort the level(s) in the resulting MultiIndex columns.

        is_series_input : bool, default False
            Whether the input is a Series, in which case we call `droplevel`
        """
        if not isinstance(level, int):
            # TODO: SNOW-1558364: Support index name passed to level parameter
            ErrorMessage.not_implemented(
                "Snowpark pandas DataFrame/Series.unstack does not yet support a non-integer `level` parameter"
            )
        if not sort:
            ErrorMessage.not_implemented(
                "Snowpark pandas DataFrame/Series.unstack does not yet support the `sort` parameter"
            )
        if self._modin_frame.is_multiindex(axis=1):
            ErrorMessage.not_implemented(
                "Snowpark pandas doesn't support multiindex columns in the unstack API"
            )

        # unstack() should preserve timedelta types, but one input column may
        # may map to multiple output columns, so we don't support timedelta
        # inputs yet.
        self._raise_not_implemented_error_for_timedelta()

        level = [level]
        index_names = self.get_index_names()

        # Check to see if we have a MultiIndex, if we do, make sure we remove
        # the appropriate level(s), and we pivot accordingly.
        if len(index_names) > 1:
            # Resetting the index keeps the index columns as the first n data columns
            qc = self.reset_index()
            index_cols = qc._modin_frame.data_column_pandas_labels[0 : len(index_names)]
            pivot_cols = [index_cols[lev] for lev in level]  # type: ignore
            res_index_cols = []
            column_names_to_reset_to_none = []
            for i in range(len(index_names)):
                if index_names[i] is None:
                    # We need to track the names where the index and columns originally had no name
                    # in order to reset those names back to None after the operation
                    column_names_to_reset_to_none.append(
                        qc._modin_frame.data_column_pandas_labels[i]
                    )
                col = index_cols[i]
                if col not in pivot_cols:
                    res_index_cols.append(col)
            vals = [
                c
                for c in self.columns
                if c not in res_index_cols and c not in pivot_cols
            ]

            qc = qc.pivot_table(
                columns=pivot_cols,
                index=res_index_cols,
                values=vals,
                aggfunc="min",
                fill_value=fill_value,
                margins=False,
                dropna=True,
                margins_name="All",
                observed=False,
                sort=sort,
            )

            # Set the original unnamed index values back to None
            output_index_names = qc.get_index_names()
            output_index_names_replace_level_with_none = [
                None
                if output_index_names[i] in column_names_to_reset_to_none
                else output_index_names[i]
                for i in range(len(output_index_names))
            ]
            qc = qc.set_index_names(output_index_names_replace_level_with_none)
            # Set the unnamed column values back to None
            output_column_names = qc.columns.names
            output_column_names_replace_level_with_none = [
                None
                if output_column_names[i] in column_names_to_reset_to_none
                else output_column_names[i]
                for i in range(len(output_column_names))
            ]
            qc = qc.set_columns(
                qc.columns.set_names(output_column_names_replace_level_with_none)
            )
        else:
            qc = self._stack_helper(operation=StackOperation.UNSTACK)

        if is_series_input and qc.columns.nlevels > 1:
            # If input is Series and output is MultiIndex, drop the top level of the MultiIndex
            qc = qc.set_columns(qc.columns.droplevel())
        return qc

    def _stack_helper(
        self,
        operation: StackOperation,
    ) -> "SnowflakeQueryCompiler":
        """
        Helper function that performs stacking or unstacking operation on single index dataframe/series.

        Parameters
        ----------
        operation : StackOperation.STACK or StackOperation.UNSTACK
            The operation being performed.
        """
        index_names = self.get_index_names()
        # Resetting the index keeps the index columns as the first n data columns
        qc = self.reset_index()
        index_cols = qc._modin_frame.data_column_pandas_labels[0 : len(index_names)]
        column_names_to_reset_to_none = []
        for i in range(len(index_names)):
            if index_names[i] is None:
                # We need to track the names where the index and columns originally had no name
                # in order to reset those names back to None after the operation
                column_names_to_reset_to_none.append(
                    qc._modin_frame.data_column_pandas_labels[i]
                )

        # Track the new column name for the original unnamed column
        if self.columns.name is None:
            quoted_col_label = (
                qc._modin_frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
                    pandas_labels=["index_second_level"]
                )[0]
            )
            col_label = unquote_name_if_quoted(quoted_col_label)
            column_names_to_reset_to_none.append(col_label)
        else:
            col_label = self.columns.name

        qc = qc.melt(
            id_vars=index_cols,  # type: ignore
            value_vars=self.columns,
            var_name=col_label,
            value_name=MODIN_UNNAMED_SERIES_LABEL,
            ignore_index=False,
        )

        if operation == StackOperation.STACK:
            # Only sort rows by column values in case of 'stack'
            # For 'unstack' maintain the row position order
            qc = qc.sort_rows_by_column_values(
                columns=index_cols,  # type: ignore
                ascending=[True],
                kind="stable",
                na_position="last",
                ignore_index=False,
            )

        if operation == StackOperation.STACK:
            qc = qc.set_index_from_columns(index_cols + [col_label])  # type: ignore
        else:
            qc = qc.set_index_from_columns([col_label] + index_cols)  # type: ignore

        # Set the original unnamed index and column values back to None
        output_index_names = qc.get_index_names()
        output_index_names = [
            None
            if output_index_names[i] in column_names_to_reset_to_none
            else output_index_names[i]
            for i in range(len(output_index_names))
        ]
        qc = qc.set_index_names(output_index_names)
        return qc

    def corr(
        self,
        method: Union[str, Callable] = "pearson",
        min_periods: Optional[int] = 1,
    ) -> "SnowflakeQueryCompiler":
        """
        Compute pairwise correlation of columns, excluding NA/null values.

        Parameters
        ----------
        method : {‘pearson’, ‘kendall’, ‘spearman’} or callable
            Method of correlation:
            pearson : standard correlation coefficient
            kendall : Kendall Tau correlation coefficient
            spearman : Spearman rank correlation
            callable: callable with input two 1d ndarrays
                and returning a float. Note that the returned matrix from corr will have 1 along the diagonals and will be symmetric regardless of the callable’s behavior.

        min_periods : int, optional
            Minimum number of observations required per pair of columns to have a valid result. Currently only available for Pearson and Spearman correlation.
        """
        if not isinstance(method, str):
            ErrorMessage.not_implemented(
                "Snowpark pandas DataFrame.corr does not yet support non-string 'method'"
            )

        if method != "pearson":
            ErrorMessage.not_implemented(
                f"Snowpark pandas DataFrame.corr does not yet support 'method={method}'"
            )

        if min_periods is None:
            min_periods = 1

        frame = self._modin_frame

        query_compilers = []
        for outer_pandas_label, outer_quoted_identifier in zip(
            frame.data_column_pandas_labels,
            frame.data_column_snowflake_quoted_identifiers,
        ):
            index_quoted_identifier = (
                frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
                    pandas_labels=[INDEX_LABEL],
                )[0]
            )

            # Apply a "min" function to the index column to make sure it's also an aggregate.
            index_col = min_(pandas_lit(outer_pandas_label)).as_(
                index_quoted_identifier
            )

            new_columns = [index_col]
            for (
                inner_quoted_identifier
            ) in frame.data_column_snowflake_quoted_identifiers:
                new_col = corr(outer_quoted_identifier, inner_quoted_identifier)
                if min_periods > 1:
                    outer_col_is_valid = builtin("count_if")(
                        col(outer_quoted_identifier).is_not_null()
                    ) >= pandas_lit(min_periods)
                    inner_col_is_valid = builtin("count_if")(
                        col(inner_quoted_identifier).is_not_null()
                    ) >= pandas_lit(min_periods)
                    new_col = iff(
                        outer_col_is_valid & inner_col_is_valid,
                        new_col,
                        pandas_lit(None),
                    )
                new_col = new_col.as_(inner_quoted_identifier)
                new_columns.append(new_col)

            new_ordered_data_frame = frame.ordered_dataframe.agg(*new_columns)

            new_frame = InternalFrame.create(
                ordered_dataframe=new_ordered_data_frame,
                data_column_pandas_labels=frame.data_column_pandas_labels,
                data_column_pandas_index_names=[None],
                data_column_snowflake_quoted_identifiers=frame.data_column_snowflake_quoted_identifiers,
                index_column_pandas_labels=[None],
                index_column_snowflake_quoted_identifiers=[index_quoted_identifier],
                data_column_types=None,
                index_column_types=[None],
            )

            query_compilers.append(SnowflakeQueryCompiler(new_frame))

        if len(query_compilers) == 1:
            result = query_compilers[0]
        else:
            result = query_compilers[0].concat(axis=0, other=query_compilers[1:])
        return result

    def compare(
        self,
        other: "SnowflakeQueryCompiler",
        align_axis: Axis,
        keep_shape: bool,
        keep_equal: bool,
        result_names: tuple[str],
    ) -> "SnowflakeQueryCompiler":
        """
        Compare to another query compiler and show the differences.

        Parameters
        ----------
        other : SnowflakeQueryCompiler
            The query compiler to compare `self` to.

        align_axis : {{0 or 'index', 1 or 'columns'}}, default 1
            Which axis to align the comparison on.

            * 0, or 'index' : Resulting differences are stacked vertically
                with rows drawn alternately from self and other.
            * 1, or 'columns' : Resulting differences are aligned horizontally
                with columns drawn alternately from self and other.

            Snowpark pandas does not yet support 1 / 'columns'.

        keep_shape : bool, default False
            If true, keep all rows.
            Otherwise, only keep rows with different values.

            Snowpark pandas does not yet support `keep_shape = True`.

        keep_equal : bool, default False
            If true, keep values that are equal.
            Otherwise, show equal values as nulls.

            Snowpark pandas does not yet support `keep_equal = True`.

        result_names : tuple, default ('self', 'other')
            How to distinguish this series's values from the other's values in
            the result.

            Snowpark pandas does not yet support names other than the default.

        Returns
        -------
        SnowflakeQueryCompiler
            The comparison result.
        """
        if align_axis not in (1, "columns"):
            ErrorMessage.not_implemented(
                "Snowpark pandas `compare` does not yet support the parameter `align_axis`."
            )
        if keep_shape:
            ErrorMessage.not_implemented(
                "Snowpark pandas `compare` does not yet support the parameter `keep_shape`."
            )
        if keep_equal:
            ErrorMessage.not_implemented(
                "Snowpark pandas `compare` does not yet support the parameter `keep_equal."
            )
        if result_names != ("self", "other"):
            ErrorMessage.not_implemented(
                "Snowpark pandas `compare` does not yet support the parameter `result_names`."
            )

        """
        In examples below, use the following query compilers with a single diff at iloc[0, 0]

        >>> df1 = pd.DataFrame(
            [
                [None, None,],
                ["a", 1,],
                ["b", 2,],
                [None, 3],
            ],
            index=pd.MultiIndex.from_tuples(
                [
                    ("row1", 1),
                    ("row1", 1),
                    ("row3", 3),
                    ("row4", 4),
                ],
                names=("row_level1", "row_level2"),
            ),
            columns=pd.MultiIndex.from_tuples(
                [
                    ("group_1", "string_col"),
                    ("group_1", "int_col"),
                ],
                names=["column_level1", "column_level2"],
            ),
            )
        >>> df1
            column_level1            group_1
            column_level2         string_col int_col
            row_level1 row_level2
            row1       1                None     NaN
                    1                   a     1.0
            row3       3                   b     2.0
            row4       4                None     3.0

        >>> df2 = pd.DataFrame(
            [
                ["c", None,],
                ["a", 1,],
                ["b", 2,],
                [None, 3],
            ],
            index=pd.MultiIndex.from_tuples(
                [
                    ("row1", 1),
                    ("row1", 1),
                    ("row3", 3),
                    ("row4", 4),
                ],
                names=("row_level1", "row_level2"),
            ),
            columns=pd.MultiIndex.from_tuples(
                [
                    ("group_1", "string_col"),
                    ("group_1", "int_col"),
                ],
                names=["column_level1", "column_level2"],
            ),
            )
        >>> df2
            column_level1            group_1
            column_level2         string_col int_col
            row_level1 row_level2
            row1       1                   c     NaN
                    1                   a     1.0
            row3       3                   b     2.0
            row4       4                None     3.0
        >>> self = df1._query_compiler
        >>> other = df2._query_compiler
        """

        if not (self.columns.equals(other.columns) and self.index.equals(other.index)):
            raise ValueError("Can only compare identically-labeled objects")

        # align the two frames on index values, which should be equal. We don't
        # align on row position because the frames might not have row position
        # columns.
        result_frame, result_column_mapper = join_utils.align(
            left=self._modin_frame,
            right=other._modin_frame,
            left_on=self._modin_frame.index_column_snowflake_quoted_identifiers,
            right_on=other._modin_frame.index_column_snowflake_quoted_identifiers,
        )

        # compare each column in `self` to the corresponding column in `other`.
        binary_op_result = result_frame
        for left_identifier, right_identifier, left_pandas_label in zip(
            self._modin_frame.data_column_snowflake_quoted_identifiers,
            other._modin_frame.data_column_snowflake_quoted_identifiers,
            self._modin_frame.data_column_pandas_labels,
        ):
            left_identiifer = result_column_mapper.left_quoted_identifiers_map[
                left_identifier
            ]
            right_identifier = result_column_mapper.right_quoted_identifiers_map[
                right_identifier
            ]
            op_result = BinaryOp.create(
                op="equal_null",
                first_operand=col(left_identifier),
                first_datatype=functools.partial(
                    lambda col: result_frame.get_snowflake_type(col), left_identiifer
                ),
                second_operand=col(right_identifier),
                second_datatype=functools.partial(
                    lambda col: result_frame.get_snowflake_type(col), right_identifier
                ),
            ).compute()
            binary_op_result = binary_op_result.append_column(
                str(left_pandas_label) + "_comparison_result",
                op_result.snowpark_column,
                op_result.snowpark_pandas_type,
            )
        """
        >>> SnowflakeQueryCompiler(binary_op_result).to_pandas()

            column_level1            group_1                            ('group_1', 'string_col')_comparison_result ('group_1', 'int_col')_comparison_result
            column_level2         string_col int_col string_col int_col                                         NaN                                      NaN
            row_level1 row_level2
            row1       1                None     NaN          c     NaN                                       False                                     True
                       1                   a     1.0          a     1.0                                        True                                     True
            row3       3                   b     2.0          b     2.0                                        True                                     True
            row4       4                None     3.0       None     3.0                                        True                                     True
        """

        # drop the rows where all the columns are equal, i.e. where all the
        # `comparison_result_columns` are true.
        comparison_result_columns = (
            binary_op_result.data_column_snowflake_quoted_identifiers[
                -len(self._modin_frame.data_column_snowflake_quoted_identifiers) :
            ]
        )
        filtered_binary_op_result = binary_op_result.filter(
            ~functools.reduce(
                lambda a, b: (a & col(b)), comparison_result_columns, pandas_lit(True)
            )
        )
        """
        In our example, we've dropped all rows but the first, which has the diff:

        >>> SnowflakeQueryCompiler(filtered_binary_op_result).to_pandas()
            column_level1            group_1                            ('group_1', 'string_col')_comparison_result ('group_1', 'int_col')_comparison_result
            column_level2         string_col int_col string_col int_col                                         NaN                                      NaN
            row_level1 row_level2
            row1       1                None     NaN          c     NaN                                       False                                     True
        """

        # Get a pandas series that tells whether each column contains only
        # matches. We need to execute at least one intermediate SQL query to
        # get this series.
        filtered_qc = SnowflakeQueryCompiler(
            get_frame_by_col_pos(
                filtered_binary_op_result,
                list(range(len(self.columns) * 2, len(self.columns) * 3)),
            )
        )
        # Even though it incurs an extra query, we must get the length of the index to prevent errors.
        # When called with an empty DF/Series, `all` can return boolean values. Here, a query compiler is
        # always expected to be returned. When the index is empty, create the required query compiler instead
        # of calling `all`.
        empty_index = filtered_qc.get_axis_len(axis=0) == 0
        if empty_index:
            qc_all = SnowflakeQueryCompiler(
                filtered_qc._modin_frame.update_snowflake_quoted_identifiers_with_expressions(
                    {
                        col_id: pandas_lit(True)
                        for col_id in filtered_qc._modin_frame.data_column_snowflake_quoted_identifiers
                    }
                ).frame
            )
        else:
            qc_all = filtered_qc.all(axis=0, bool_only=False, skipna=False)
        all_rows_match_frame = qc_all.to_pandas()
        """
        In our example, we find that the second columns of each frame match
        completely, but the first columns do not:

        >>> all_rows_match_frame

                                                                       __reduced__
            column_level1                               column_level2
            ('group_1', 'string_col')_comparison_result NaN                  False
            ('group_1', 'int_col')_comparison_result    NaN                   True
        """

        # Construct expressions for the result.
        new_pandas_labels = []
        new_values = []
        column_index_tuples = []
        column_types = []
        for (
            pandas_column_value,
            pandas_label,
            left_identifier,
            right_identifier,
            column_only_contains_matches,
            left_type,
            right_type,
        ) in zip(
            self.columns,
            filtered_binary_op_result.data_column_pandas_labels,
            self._modin_frame.data_column_snowflake_quoted_identifiers,
            other._modin_frame.data_column_snowflake_quoted_identifiers,
            all_rows_match_frame.iloc[:, 0].values,
            self._modin_frame.cached_data_column_snowpark_pandas_types,
            other._modin_frame.cached_data_column_snowpark_pandas_types,
        ):
            # Drop columns that only contain matches.
            if column_only_contains_matches:
                continue

            left_mappped_identifier = result_column_mapper.left_quoted_identifiers_map[
                left_identifier
            ]
            right_mapped_identifier = result_column_mapper.right_quoted_identifiers_map[
                right_identifier
            ]

            cols_equal = (
                BinaryOp.create(
                    op="equal_null",
                    first_operand=col(left_mappped_identifier),
                    first_datatype=functools.partial(
                        lambda col: result_frame.get_snowflake_type(col),
                        left_mappped_identifier,
                    ),
                    second_operand=col(right_mapped_identifier),
                    second_datatype=functools.partial(
                        lambda col: result_frame.get_snowflake_type(col),
                        right_mapped_identifier,
                    ),
                )
                .compute()
                .snowpark_column
            )

            # Add a column containing the values from `self`, but replace
            # matching values with null.
            new_pandas_labels.append(pandas_label)
            new_values.append(
                iff(
                    condition=cols_equal,
                    expr1=pandas_lit(np.nan),
                    expr2=col(left_mappped_identifier),
                )
            )

            # Add a column containing the values from `other`, but replace
            # matching values with null.
            new_pandas_labels.append(pandas_label)
            new_values.append(
                iff(
                    condition=cols_equal,
                    expr1=pandas_lit(np.nan),
                    expr2=col(right_mapped_identifier),
                )
            )

            # Add the two column labels of the result: these are the same as
            # self's column labels, except with an extra level telling whether
            # the column came from `self` or `other`.
            if self.columns.nlevels > 1:
                column_index_tuples.append((*pandas_column_value, "self"))
                column_index_tuples.append((*pandas_column_value, "other"))
            else:
                column_index_tuples.append((pandas_column_value, "self"))
                column_index_tuples.append((pandas_column_value, "other"))

            column_types.append(left_type)
            column_types.append(right_type)

        result = SnowflakeQueryCompiler(
            filtered_binary_op_result.project_columns(
                new_pandas_labels, new_values, column_types
            )
        ).set_columns(
            # TODO(SNOW-1510921): fix the levels and inferred_type of the
            # result's MultiIndex once we can pass the levels correctly through
            # set_columns.
            pd.MultiIndex.from_tuples(
                column_index_tuples, names=(*self.columns.names, None)
            )
        )

        """
        In our example, the final result keeps only one pair of columns and one
        row for the single diff:

        >>> result.to_pandas()
            column_level1            group_1
            column_level2         string_col
                                        self other
            row_level1 row_level2
            row1       1                None     c
        """

        return result

    def tz_convert(
        self,
        tz: Union[str, tzinfo],
        axis: int = 0,
        level: Optional[Level] = None,
        copy: bool = True,
    ) -> "SnowflakeQueryCompiler":
        """
        Convert tz-aware axis to target time zone.

        Parameters
        ----------
        tz : str or tzinfo object or None
            Target time zone. Passing None will convert to UTC and remove the timezone information.
        axis : {0 or ‘index’, 1 or ‘columns’}, default 0
            The axis to convert
        level : int, str, default None
            If axis is a MultiIndex, convert a specific level. Otherwise must be None.
        copy : bool, default True
            Also make a copy of the underlying data.

        Returns
        -------
        SnowflakeQueryCompiler
            The result of applying time zone conversion.
        """
        if axis in (1, "columns"):
            ErrorMessage.not_implemented(
                f"Snowpark pandas 'tz_convert' method doesn't yet support 'axis={axis}'"
            )
        if level is not None:
            ErrorMessage.not_implemented(
                "Snowpark pandas 'tz_convert' method doesn't yet support the 'level' parameter"
            )
        if copy is not True:
            ErrorMessage.not_implemented(
                "Snowpark pandas 'tz_convert' method doesn't support 'copy=False'"
            )
        if isinstance(tz, str) and tz not in pytz.all_timezones:
            ErrorMessage.not_implemented(
                f"Snowpark pandas 'tz_convert' method doesn't support 'tz={tz}'"
            )

        return SnowflakeQueryCompiler(
            self._modin_frame.apply_snowpark_function_to_columns(
                lambda column: tz_convert_column(column, tz),
                include_data=False,
                include_index=True,
            )
        )

    def tz_localize(
        self,
        tz: Union[str, tzinfo],
        axis: int = 0,
        level: Optional[Level] = None,
        copy: bool = True,
        ambiguous: str = "raise",
        nonexistent: str = "raise",
    ) -> "SnowflakeQueryCompiler":
        """
        Localize tz-naive index of a Series or DataFrame to target time zone.

        This operation localizes the Index. To localize the values in a timezone-naive Series, use Series.dt.tz_localize().

        Parameters
        ----------
        tz : str or tzinfo or None
            Time zone to localize. Passing None will remove the time zone information and preserve local time.
        axis : {0 or ‘index’, 1 or ‘columns’}, default 0
            The axis to localize
        level : int, str, default None
            If axis is a MultiIndex, localize a specific level. Otherwise must be None.
        copy : bool, default True
            Also make a copy of the underlying data.
        ambiguous: ‘infer’, bool-ndarray, ‘NaT’, default ‘raise’
            When clocks moved backward due to DST, ambiguous times may arise. For example in Central European Time (UTC+01), when going from 03:00 DST to 02:00 non-DST, 02:30:00 local time occurs both at 00:30:00 UTC and at 01:30:00 UTC. In such a situation, the ambiguous parameter dictates how ambiguous times should be handled.
            - ‘infer’ will attempt to infer fall dst-transition hours based on order
            - bool-ndarray where True signifies a DST time, False designates a non-DST time (note that this flag is only applicable for ambiguous times)
            - ‘NaT’ will return NaT where there are ambiguous times
            - ‘raise’ will raise an AmbiguousTimeError if there are ambiguous times.
        nonexistent : str, default ‘raise’
            A nonexistent time does not exist in a particular timezone where clocks moved forward due to DST. Valid values are:
            - ‘shift_forward’ will shift the nonexistent time forward to the closest existing time
            - ‘shift_backward’ will shift the nonexistent time backward to the closest existing time
            - ‘NaT’ will return NaT where there are nonexistent times
            - timedelta objects will shift nonexistent times by the timedelta
            - ‘raise’ will raise an NonExistentTimeError if there are nonexistent times.

        Returns
        -------
        SnowflakeQueryCompiler
            The result of applying time zone localization.
        """
        if axis in (1, "columns"):
            ErrorMessage.not_implemented(
                f"Snowpark pandas 'tz_localize' method doesn't yet support 'axis={axis}'"
            )
        if level is not None:
            ErrorMessage.not_implemented(
                "Snowpark pandas 'tz_localize' method doesn't yet support the 'level' parameter"
            )
        if copy is not True:
            ErrorMessage.not_implemented(
                "Snowpark pandas 'tz_localize' method doesn't support 'copy=False'"
            )
        if not isinstance(ambiguous, str) or ambiguous != "raise":
            ErrorMessage.not_implemented(
                "Snowpark pandas 'tz_localize' method doesn't yet support the 'ambiguous' parameter"
            )
        if not isinstance(nonexistent, str) or nonexistent != "raise":
            ErrorMessage.not_implemented(
                "Snowpark pandas 'tz_localize' method doesn't yet support the 'nonexistent' parameter"
            )
        if isinstance(tz, str) and tz not in pytz.all_timezones:
            ErrorMessage.not_implemented(
                f"Snowpark pandas 'tz_localize' method doesn't support 'tz={tz}'"
            )

        return SnowflakeQueryCompiler(
            self._modin_frame.apply_snowpark_function_to_columns(
                lambda column: tz_localize_column(column, tz),
                include_data=False,
                include_index=True,
            )
        )

    def timedelta_property(
        self, property_name: str, include_index: bool = False
    ) -> "SnowflakeQueryCompiler":
        """
        Extract a specified component of from Timedelta.

        Parameters
        ----------
        property : {'days', 'seconds', 'microseconds', 'nanoseconds'}
            The component to extract.
        include_index: Whether to include the index columns in the operation.

        Returns
        -------
        A new SnowflakeQueryCompiler with the extracted component.
        """
        if not include_index:
            assert (
                len(self.columns) == 1
            ), "dt only works for series"  # pragma: no cover
            if is_datetime64_any_dtype(self.dtypes[0]):
                raise AttributeError(
                    f"'DatetimeProperties' object has no attribute '{property_name}'"
                )

        # mapping from the property name to the corresponding snowpark function
        property_to_func_map = {
            "days": lambda column: trunc(column / NANOSECONDS_PER_DAY),
            "seconds": lambda column: trunc(column / NANOSECONDS_PER_SECOND)
            % SECONDS_PER_DAY,
            "microseconds": lambda column: trunc(column / NANOSECONDS_PER_MICROSECOND)
            % MICROSECONDS_PER_SECOND,
            "nanoseconds": lambda column: column % NANOSECONDS_PER_MICROSECOND,
        }
        func = property_to_func_map.get(property_name)
        if not func:
            class_prefix = (
                "TimedeltaIndex" if include_index else "Series.dt"
            )  # pragma: no cover
            raise ErrorMessage.not_implemented(
                f"Snowpark pandas doesn't yet support the property '{class_prefix}.{property_name}'"
            )  # pragma: no cover
        return SnowflakeQueryCompiler(
            self._modin_frame.apply_snowpark_function_to_columns(
                func,
                include_index=include_index,
            )
        )

    def groupby_unique(
        self,
        by: Any,
        axis: int,
        groupby_kwargs: dict,
        agg_args: Sequence,
        agg_kwargs: dict,
        numeric_only: bool,
        is_series_groupby: bool,
        drop: bool = False,
    ) -> "SnowflakeQueryCompiler":
        """
        Aggregate unique values for each group into a list.


        Parameters
        ----------
        by : Any
            Index level name(s) or column label(s) to group by.
        axis: int
            The axis along which to group data. This parameter must be 0, but
            we keep it to match the interface of Modin's BaseQueryCompiler.
        groupby_kwargs: dict
            The keyword arguments to groupby().
        agg_args: Sequence
            Positional arguments to the unique() aggregation function. This
            parameter must be empty because unique() does not take positional
            arguments, but we keep the parameter to match the interface of
            Modin's BaseQueryCompiler.
        agg_kwargs: dict
            Keyword arguments to the unique() aggregation function. This
            parameter must be empty because unique() does not take keyword
            arguments, but we keep the parameter to match the interface of
            Modin's BaseQueryCompiler.
        numeric_only: bool
            This parameter is meaningless as unique() does not take a
            numeric_only parameter, but we keep the parameter to match the
            interface of Modin's BaseQueryCompiler.
        is_series_groupby: bool
            Whether this method is called via SeriesGroupBy as opposed to
            DataFrameGroupBy. This parameter should always be true, but we keep
            it to match the interface of Modin's BaseQueryCompiler.
        drop: bool, default False
            Whether the `by` columns are internal this dataframe.

        Returns
        -------
        A new SnowflakeQueryCompiler with the unique values of the singular
        data column for each group.
        """
        assert axis == 0, "Internal error. SeriesGroupBy.unique() axis should be 0."
        assert len(agg_args) == 0, (
            "Internal error. SeriesGroupBy.unique() does not take "
            + "aggregation arguments."
        )
        assert len(agg_kwargs) == 0, (
            "Internal error. SeriesGroupBy.unique() does not take "
            + "aggregation arguments."
        )
        assert (
            is_series_groupby is True
        ), "Internal error. Only SeriesGroupBy has a unique() method."

        compiler = SnowflakeQueryCompiler(
            self._modin_frame.ensure_row_position_column()
        )
        compiler, by_list = resample_and_extract_groupby_column_pandas_labels(
            compiler, by, groupby_kwargs.get("level", None)
        )
        by_snowflake_quoted_identifiers_list = []
        for (
            entry
        ) in compiler._modin_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
            by_list
        ):
            assert len(entry) == 1, (
                "Internal error. Each grouping label should correspond to a "
                + "single Snowpark column."
            )
            by_snowflake_quoted_identifiers_list.append(entry[0])

        # There is no built-in snowflake function to aggregation unique values
        # of a column into array while preserving a certain order. We implement
        # the aggregation in the following steps:
        # 1) Project a new column representing the row position of each row
        #    within each combination of group + data column value.
        # 2) Filter the result to the rows where the new column is equal to 1,
        #    i.e. get the row where each data column value appears for the first
        #    time within each group.
        # 3) Project away the extra rank column.
        # 4) Group according to `groupby_kwargs` and for each group, aggregate
        #    the (singular) remaining data column into a list ordered by the
        #    original row order.
        frame_with_rank = compiler._modin_frame.append_column(
            "_rank_column",
            rank().over(
                Window.partition_by(
                    *by_snowflake_quoted_identifiers_list,
                    *(
                        identifier
                        for identifier in (
                            compiler._modin_frame.data_column_snowflake_quoted_identifiers
                        )
                        if identifier not in by_snowflake_quoted_identifiers_list
                    ),
                ).order_by(
                    compiler._modin_frame.row_position_snowflake_quoted_identifier
                )
            ),
        )
        return (
            SnowflakeQueryCompiler(
                frame_with_rank.filter(
                    col(frame_with_rank.data_column_snowflake_quoted_identifiers[-1])
                    == 1
                )
            )
            .take_2d_positional(
                index=slice(None),
                columns=(
                    list(
                        range(
                            len(
                                frame_with_rank.data_column_snowflake_quoted_identifiers
                            )
                            - 1
                        )
                    )
                ),
            )
            .groupby_agg(
                by=by,
                agg_func="array_agg",
                axis=axis,
                groupby_kwargs=groupby_kwargs,
                agg_args=agg_args,
                agg_kwargs=agg_kwargs,
                numeric_only=numeric_only,
                is_series_groupby=is_series_groupby,
                drop=drop,
            )
        )

    def hist_on_series(
        self,
        by: object = None,
        xlabelsize: Optional[int] = None,
        xrot: Optional[float] = None,
        ylabelsize: Optional[int] = None,
        yrot: Optional[float] = None,
        figsize: Optional[tuple[int, int]] = None,
        bins: Union[int, Sequence[int]] = 10,
        backend: Optional[str] = None,
        legend: bool = False,
        **kwargs: dict[str, Any],
    ) -> tuple["SnowflakeQueryCompiler", float, float, float]:
        """
        Draw histogram of the input series using matplotlib.

        Parameters
        ----------
        by : object, optional
            If passed, then used to form histograms for separate groups.
        xlabelsize : int, default None
            If specified changes the x-axis label size.
        xrot : float, default None
            Rotation of x axis labels.
        ylabelsize : int, default None
            If specified changes the y-axis label size.
        yrot : float, default None
            Rotation of y axis labels.
        figsize : tuple, default None
            Figure size in inches by default.
        bins : int or sequence, default 10
            Number of histogram bins to be used. If an integer is given, bins + 1 bin edges are calculated and returned. If bins is a sequence, gives bin edges, including left edge of first bin and right edge of last bin. In this case, bins is returned unmodified.
        backend : str, default None
            Backend to use instead of the backend specified in the option plotting.backend. For instance, ‘matplotlib’. Alternatively, to specify the plotting.backend for the whole session, set pd.options.plotting.backend.
        legend : bool, default False
            Whether to show the legend.
        **kwargs
            To be passed to the actual plotting function.

        Returns
        -------
        A tuple containing the following in order:
            1) A SnowflakeQueryCompiler representing the count of each group in the histogram
            2) The minimum value in the series
            3) The maximum value in the series
            4) The bin size
        """
        if by is not None:
            ErrorMessage.parameter_not_implemented_error("by", "Series.hist")
        if xlabelsize is not None:
            ErrorMessage.parameter_not_implemented_error("xlabelsize", "Series.hist")
        if xrot is not None:
            ErrorMessage.parameter_not_implemented_error("xrot", "Series.hist")
        if ylabelsize is not None:
            ErrorMessage.parameter_not_implemented_error("ylabelsize", "Series.hist")
        if yrot is not None:
            ErrorMessage.parameter_not_implemented_error("yrot", "Series.hist")
        if figsize is not None:
            ErrorMessage.parameter_not_implemented_error("figsize", "Series.hist")
        if not isinstance(bins, int):
            ErrorMessage.not_implemented(
                "Snowpark pandas 'Series.hist' method does not yet support the 'bins' parameter with types other than 'int'"
            )
        elif bins <= 0:
            raise ValueError("`bins` must be positive, when an integer")
        if backend is not None:
            ErrorMessage.parameter_not_implemented_error("backend", "Series.hist")
        if legend:
            ErrorMessage.parameter_not_implemented_error("legend", "Series.hist")

        assert (
            len(self.columns) == 1
        ), "Internal error: this query compiler should represent a series."
        hist_col = col(self._modin_frame.data_column_snowflake_quoted_identifiers[0])
        [min_val, max_val] = self._modin_frame.ordered_dataframe.agg(
            min_(hist_col).as_("min_value"),
            max_(hist_col).as_("max_value"),
        ).collect()[0]

        bin_size = (max_val - min_val) / bins

        frame_with_binned_column = self._modin_frame.append_column(
            "_binned_column",
            min_val
            + bin_size
            * floor(
                (iff(hist_col == max_val, max_val - bin_size, hist_col) - min_val)
                / bin_size
            ),
        )

        count_quoted_identifier = frame_with_binned_column.ordered_dataframe.generate_snowflake_quoted_identifiers(
            pandas_labels=["count"],
        )[
            0
        ]

        groupby_quoted_identifier = (
            frame_with_binned_column.data_column_snowflake_quoted_identifiers[-1]
        )
        new_ordered_dataframe = frame_with_binned_column.ordered_dataframe.group_by(
            [groupby_quoted_identifier],
            count(col("*")).alias(count_quoted_identifier),
        )

        qc = SnowflakeQueryCompiler(
            InternalFrame.create(
                ordered_dataframe=new_ordered_dataframe,
                data_column_pandas_labels=["count"],
                data_column_snowflake_quoted_identifiers=[count_quoted_identifier],
                data_column_pandas_index_names=frame_with_binned_column.data_column_pandas_index_names,
                index_column_pandas_labels=[None],
                index_column_snowflake_quoted_identifiers=[groupby_quoted_identifier],
                data_column_types=[None],
                index_column_types=[None],
            )
        )
        return (qc, min_val, max_val, bin_size)

    def create_or_replace_view(
        self,
        name: Union[str, Iterable[str]],
        *,
        comment: Optional[str] = None,
        index: bool = True,
        index_label: Optional[IndexLabel] = None,
    ) -> List[Row]:
        snowpark_df = self._to_snowpark_dataframe_from_snowpark_pandas_dataframe(
            index, index_label
        )

        return snowpark_df.create_or_replace_view(
            name=name,
            comment=comment,
            statement_params=get_default_snowpark_pandas_statement_params(),
        )

    def create_or_replace_dynamic_table(
        self,
        name: Union[str, Iterable[str]],
        *,
        warehouse: str,
        lag: str,
        comment: Optional[str] = None,
        mode: str = "overwrite",
        refresh_mode: Optional[str] = None,
        initialize: Optional[str] = None,
        clustering_keys: Optional[Iterable[ColumnOrName]] = None,
        is_transient: bool = False,
        data_retention_time: Optional[int] = None,
        max_data_extension_time: Optional[int] = None,
        iceberg_config: Optional[dict] = None,
        index: bool = True,
        index_label: Optional[IndexLabel] = None,
    ) -> List[Row]:
        snowpark_df = self._to_snowpark_dataframe_from_snowpark_pandas_dataframe(
            index, index_label
        )

        return snowpark_df.create_or_replace_dynamic_table(
            name=name,
            warehouse=warehouse,
            lag=lag,
            comment=comment,
            mode=mode,
            refresh_mode=refresh_mode,
            initialize=initialize,
            clustering_keys=clustering_keys,
            is_transient=is_transient,
            data_retention_time=data_retention_time,
            max_data_extension_time=max_data_extension_time,
            statement_params=get_default_snowpark_pandas_statement_params(),
            iceberg_config=iceberg_config,
        )

    def to_iceberg(
        self,
        table_name: Union[str, Iterable[str]],
        *,
        iceberg_config: dict,
        mode: Optional[str] = None,
        column_order: str = "index",
        clustering_keys: Optional[Iterable[ColumnOrName]] = None,
        block: bool = True,
        comment: Optional[str] = None,
        enable_schema_evolution: Optional[bool] = None,
        data_retention_time: Optional[int] = None,
        max_data_extension_time: Optional[int] = None,
        change_tracking: Optional[bool] = None,
        copy_grants: bool = False,
        index: bool = True,
        index_label: Optional[IndexLabel] = None,
    ) -> List[Row]:
        """
        Writes the data to the specified iceberg table in a Snowflake database.
        """
        snowpark_df = self._to_snowpark_dataframe_from_snowpark_pandas_dataframe(
            index, index_label
        )

        return snowpark_df.write.save_as_table(
            table_name=table_name,
            mode=mode,
            column_order=column_order,
            clustering_keys=clustering_keys,
            statement_params=get_default_snowpark_pandas_statement_params(),
            block=block,
            comment=comment,
            enable_schema_evolution=enable_schema_evolution,
            data_retention_time=data_retention_time,
            max_data_extension_time=max_data_extension_time,
            change_tracking=change_tracking,
            copy_grants=copy_grants,
            iceberg_config=iceberg_config,
        )
