#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""
Methods defined on BasePandasDataset that are overridden in Snowpark pandas. Adding a method to this file
should be done with discretion, and only when relevant changes cannot be made to the query compiler or
upstream frontend to accommodate Snowpark pandas.

If you must override a method in this file, please add a comment describing why it must be overridden,
and if possible, whether this can be reconciled with upstream Modin.
"""
from __future__ import annotations

import copy
import pickle as pkl
import warnings
from collections.abc import Sequence
from typing import Any, Callable, Hashable, Literal, Mapping, get_args

import modin.pandas as pd
import numpy as np
import numpy.typing as npt
import pandas
from modin.pandas import Series
from modin.pandas.api.extensions import (
    register_dataframe_accessor,
    register_series_accessor,
)
from modin.pandas.base import BasePandasDataset
from modin.pandas.utils import is_scalar
from pandas._libs import lib
from pandas._libs.lib import NoDefault, is_bool, no_default
from pandas._typing import (
    AggFuncType,
    AnyArrayLike,
    Axes,
    Axis,
    CompressionOptions,
    FillnaOptions,
    IgnoreRaise,
    IndexKeyFunc,
    IndexLabel,
    Level,
    NaPosition,
    RandomState,
    Scalar,
    StorageOptions,
    TimedeltaConvertibleTypes,
    TimestampConvertibleTypes,
)
from pandas.core.common import apply_if_callable
from pandas.core.dtypes.common import (
    is_dict_like,
    is_dtype_equal,
    is_list_like,
    is_numeric_dtype,
    pandas_dtype,
)
from pandas.core.dtypes.inference import is_integer
from pandas.core.methods.describe import _refine_percentiles
from pandas.errors import SpecificationError
from pandas.util._validators import (
    validate_ascending,
    validate_bool_kwarg,
    validate_percentile,
)

from snowflake.snowpark.modin.plugin._typing import ListLike
from snowflake.snowpark.modin.plugin.extensions.utils import (
    ensure_index,
    extract_validate_and_try_convert_named_aggs_from_kwargs,
    get_as_shape_compatible_dataframe_or_series,
    raise_if_native_pandas_objects,
    validate_and_try_convert_agg_func_arg_func_to_str,
)
from snowflake.snowpark.modin.plugin.utils.error_message import (
    ErrorMessage,
    base_not_implemented,
)
from snowflake.snowpark.modin.plugin.utils.warning_message import (
    WarningMessage,
    materialization_warning,
)
from snowflake.snowpark.modin.utils import validate_int_kwarg

_TIMEDELTA_PCT_CHANGE_AXIS_1_MIXED_TYPE_ERROR_MESSAGE = (
    "pct_change(axis=1) is invalid when one column is Timedelta another column is not."
)


def register_base_override(method_name: str):
    """
    Decorator function to override a method on BasePandasDataset. Since Modin does not provide a mechanism
    for directly overriding methods on BasePandasDataset, we mock this by performing the override on
    DataFrame and Series, and manually performing a `setattr` on the base class. These steps are necessary
    to allow both the docstring extension and method dispatch to work properly.
    """

    def decorator(base_method: Any):
        parent_method = getattr(BasePandasDataset, method_name, None)
        if isinstance(parent_method, property):
            parent_method = parent_method.fget
        # If the method was not defined on Series/DataFrame and instead inherited from the superclass
        # we need to override it as well.
        series_method = getattr(pd.Series, method_name, None)
        if isinstance(series_method, property):
            series_method = series_method.fget
        if (
            series_method is None
            or series_method is parent_method
            or parent_method is None
        ):
            register_series_accessor(method_name)(base_method)
        df_method = getattr(pd.DataFrame, method_name, None)
        if isinstance(df_method, property):
            df_method = df_method.fget
        if df_method is None or df_method is parent_method or parent_method is None:
            register_dataframe_accessor(method_name)(base_method)
        # Replace base method
        setattr(BasePandasDataset, method_name, base_method)
        return base_method

    return decorator


def register_base_not_implemented():
    def decorator(base_method: Any):
        func = base_not_implemented()(base_method)
        register_series_accessor(base_method.__name__)(func)
        register_dataframe_accessor(base_method.__name__)(func)
        return func

    return decorator


# === UNIMPLEMENTED METHODS ===
# The following methods are not implemented in Snowpark pandas, and must be overridden on the
# frontend. These methods fall into a few categories:
# 1. Would work in Snowpark pandas, but we have not tested it.
# 2. Would work in Snowpark pandas, but requires more SQL queries than we are comfortable with.
# 3. Requires materialization (usually via a frontend _default_to_pandas call).
# 4. Performs operations on a native pandas Index object that are nontrivial for Snowpark pandas to manage.


@register_base_not_implemented()
def asof(self, where, subset=None):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def at_time(self, time, asof=False, axis=None):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def between_time(
    self: BasePandasDataset,
    start_time,
    end_time,
    inclusive: str | None = None,
    axis=None,
):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def bool(self):  # noqa: RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def clip(
    self, lower=None, upper=None, axis=None, inplace=False, *args, **kwargs
):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def combine(self, other, func, fill_value=None, **kwargs):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def combine_first(self, other):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def droplevel(self, level, axis=0):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def explode(self, column, ignore_index: bool = False):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def ewm(
    self,
    com: float | None = None,
    span: float | None = None,
    halflife: float | TimedeltaConvertibleTypes | None = None,
    alpha: float | None = None,
    min_periods: int | None = 0,
    adjust: bool = True,
    ignore_na: bool = False,
    axis: Axis = 0,
    times: str | np.ndarray | BasePandasDataset | None = None,
    method: str = "single",
) -> pandas.core.window.ewm.ExponentialMovingWindow:  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def filter(
    self, items=None, like=None, regex=None, axis=None
):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def infer_objects(self, copy: bool | None = None):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def interpolate(
    self,
    method="linear",
    *,
    axis=0,
    limit=None,
    inplace=False,
    limit_direction: str | None = None,
    limit_area=None,
    downcast=lib.no_default,
    **kwargs,
):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def kurt(
    self, axis=no_default, skipna=True, numeric_only=False, **kwargs
):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


register_base_override("kurtosis")(kurt)


@register_base_not_implemented()
def mode(self, axis=0, numeric_only=False, dropna=True):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def pipe(self, func, *args, **kwargs):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def reindex_like(
    self, other, method=None, copy=True, limit=None, tolerance=None
):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def reorder_levels(self, order, axis=0):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def sem(
    self,
    axis: Axis | None = None,
    skipna: bool = True,
    ddof: int = 1,
    numeric_only=False,
    **kwargs,
):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def set_flags(
    self, *, copy: bool = False, allows_duplicate_labels: bool | None = None
):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def swapaxes(self, axis1, axis2, copy=True):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def swaplevel(self, i=-2, j=-1, axis=0):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def to_clipboard(
    self, excel=True, sep=None, **kwargs
):  # pragma: no cover  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def to_excel(
    self,
    excel_writer,
    sheet_name="Sheet1",
    na_rep="",
    float_format=None,
    columns=None,
    header=True,
    index=True,
    index_label=None,
    startrow=0,
    startcol=0,
    engine=None,
    merge_cells=True,
    encoding=no_default,
    inf_rep="inf",
    verbose=no_default,
    freeze_panes=None,
    storage_options: StorageOptions = None,
):  # pragma: no cover  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def to_hdf(
    self, path_or_buf, key, format="table", **kwargs
):  # pragma: no cover  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def to_json(
    self,
    path_or_buf=None,
    orient=None,
    date_format=None,
    double_precision=10,
    force_ascii=True,
    date_unit="ms",
    default_handler=None,
    lines=False,
    compression="infer",
    index=True,
    indent=None,
    storage_options: StorageOptions = None,
):  # pragma: no cover  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def to_latex(
    self,
    buf=None,
    columns=None,
    col_space=None,
    header=True,
    index=True,
    na_rep="NaN",
    formatters=None,
    float_format=None,
    sparsify=None,
    index_names=True,
    bold_rows=False,
    column_format=None,
    longtable=None,
    escape=None,
    encoding=None,
    decimal=".",
    multicolumn=None,
    multicolumn_format=None,
    multirow=None,
    caption=None,
    label=None,
    position=None,
):  # pragma: no cover  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def to_markdown(
    self,
    buf=None,
    mode: str = "wt",
    index: bool = True,
    storage_options: StorageOptions = None,
    **kwargs,
):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def to_pickle(
    self,
    path,
    compression: CompressionOptions = "infer",
    protocol: int = pkl.HIGHEST_PROTOCOL,
    storage_options: StorageOptions = None,
):  # pragma: no cover  # noqa: PR01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def to_sql(
    self,
    name,
    con,
    schema=None,
    if_exists="fail",
    index=True,
    index_label=None,
    chunksize=None,
    dtype=None,
    method=None,
):  # noqa: PR01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def to_timestamp(
    self, freq=None, how="start", axis=0, copy=True
):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def to_xarray(self):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def transform(self, func, axis=0, *args, **kwargs):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def truncate(
    self, before=None, after=None, axis=None, copy=True
):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def update(self, other) -> None:  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def xs(
    self,
    key,
    axis=0,
    level=None,
    drop_level: bool = True,
):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_base_not_implemented()
def __finalize__(self, other, method=None, **kwargs):
    pass  # pragma: no cover


@register_base_not_implemented()
def __sizeof__(self):
    pass  # pragma: no cover


# === OVERRIDDEN METHODS ===
# The below methods have their frontend implementations overridden compared to the version present
# in base.py. This is usually for one of the following reasons:
# 1. The underlying QC interface used differs from that of modin. Notably, this applies to aggregate
#    and binary operations; further work is needed to refactor either our implementation or upstream
#    modin's implementation.
# 2. Modin performs extra validation queries that perform extra SQL queries. Some of these are already
#    fixed on main; see https://github.com/modin-project/modin/issues/7340 for details.
# 3. Upstream Modin defaults to pandas for some edge cases. Defaulting to pandas at the query compiler
#    layer is acceptable because we can force the method to raise NotImplementedError, but if a method
#    defaults at the frontend, Modin raises a warning and performs the operation by coercing the
#    dataset to a native pandas object. Removing these is tracked by
#    https://github.com/modin-project/modin/issues/7104
# 4. Snowpark pandas uses different default arguments from modin. This occurs if some parameters are
#    only partially supported (like `numeric_only=True` for `skew`), but this behavior should likewise
#    be revisited.

# `aggregate` for axis=1 is performed as a call to `BasePandasDataset.apply` in upstream Modin,
# which is unacceptable for Snowpark pandas. Upstream Modin should be changed to allow the query
# compiler or a different layer to control dispatch.
@register_base_override("aggregate")
def aggregate(
    self, func: AggFuncType = None, axis: Axis | None = 0, *args: Any, **kwargs: Any
):
    """
    Aggregate using one or more operations over the specified axis.
    """
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    origin_axis = axis
    axis = self._get_axis_number(axis)

    if axis == 1 and isinstance(self, Series):
        raise ValueError(f"No axis named {origin_axis} for object type Series")

    if len(self._query_compiler.columns) == 0:
        # native pandas raise error with message "no result", here we raise a more readable error.
        raise ValueError("No column to aggregate on.")

    # If we are using named kwargs, then we do not clear the kwargs (need them in the QC for processing
    # order, as well as formatting error messages.)
    uses_named_kwargs = False
    # If aggregate is called on a Series, named aggregations can be passed in via a dictionary
    # to func.
    if func is None or (is_dict_like(func) and not self._is_dataframe):
        if axis == 1:
            raise ValueError(
                "`func` must not be `None` when `axis=1`. Named aggregations are not supported with `axis=1`."
            )
        if func is not None:
            # If named aggregations are passed in via a dictionary to func, then we
            # ignore the kwargs.
            if any(is_dict_like(value) for value in func.values()):
                # We can only get to this codepath if self is a Series, and func is a dictionary.
                # In this case, if any of the values of func are themselves dictionaries, we must raise
                # a Specification Error, as that is what pandas does.
                raise SpecificationError("nested renamer is not supported")
            kwargs = func
        func = extract_validate_and_try_convert_named_aggs_from_kwargs(
            self, allow_duplication=False, axis=axis, **kwargs
        )
        uses_named_kwargs = True
    else:
        func = validate_and_try_convert_agg_func_arg_func_to_str(
            agg_func=func,
            obj=self,
            allow_duplication=False,
            axis=axis,
        )

    # This is to stay consistent with pandas result format, when the func is single
    # aggregation function in format of callable or str, reduce the result dimension to
    # convert dataframe to series, or convert series to scalar.
    # Note: When named aggregations are used, the result is not reduced, even if there
    # is only a single function.
    # needs_reduce_dimension cannot be True if we are using named aggregations, since
    # the values for func in that case are either NamedTuples (AggFuncWithLabels) or
    # lists of NamedTuples, both of which are list like.
    need_reduce_dimension = (
        (callable(func) or isinstance(func, str))
        # A Series should be returned when a single scalar string/function aggregation function, or a
        # dict of scalar string/functions is specified. In all other cases (including if the function
        # is a 1-element list), the result is a DataFrame.
        #
        # The examples below have axis=1, but the same logic is applied for axis=0.
        # >>> df = pd.DataFrame({"a": [0, 1], "b": [2, 3]})
        #
        # single aggregation: return Series
        # >>> df.agg("max", axis=1)
        # 0    2
        # 1    3
        # dtype: int64
        #
        # list of aggregations: return DF
        # >>> df.agg(["max"], axis=1)
        #    max
        # 0    2
        # 1    3
        #
        # dict where all aggregations are strings: return Series
        # >>> df.agg({1: "max", 0: "min"}, axis=1)
        # 1    3
        # 0    0
        # dtype: int64
        #
        # dict where one element is a list: return DF
        # >>> df.agg({1: "max", 0: ["min"]}, axis=1)
        #    max  min
        # 1  3.0  NaN
        # 0  NaN  0.0
        or (
            is_dict_like(func)
            and all(not is_list_like(value) for value in func.values())
        )
    )

    # If func is a dict, pandas will not respect kwargs for each aggregation function, and
    # we should drop them before passing the to the query compiler.
    #
    # >>> native_pd.DataFrame({"a": [0, 1], "b": [np.nan, 0]}).agg("max", skipna=False, axis=1)
    # 0    NaN
    # 1    1.0
    # dtype: float64
    # >>> native_pd.DataFrame({"a": [0, 1], "b": [np.nan, 0]}).agg(["max"], skipna=False, axis=1)
    #    max
    # 0  0.0
    # 1  1.0
    # >>> pd.DataFrame([[np.nan], [0]]).aggregate("count", skipna=True, axis=0)
    # 0    1
    # dtype: int8
    # >>> pd.DataFrame([[np.nan], [0]]).count(skipna=True, axis=0)
    # TypeError: got an unexpected keyword argument 'skipna'
    if is_dict_like(func) and not uses_named_kwargs:
        kwargs.clear()

    result = self.__constructor__(
        query_compiler=self._query_compiler.agg(
            func=func,
            axis=axis,
            args=args,
            kwargs=kwargs,
        )
    )

    if need_reduce_dimension:
        if self._is_dataframe:
            result = Series(query_compiler=result._query_compiler)

        if isinstance(result, Series):
            # When func is just "quantile" with a scalar q, result has quantile value as name
            q = kwargs.get("q", 0.5)
            if func == "quantile" and is_scalar(q):
                result.name = q
            else:
                result.name = None

        # handle case for single scalar (same as result._reduce_dimension())
        if isinstance(self, Series):
            return result.to_pandas().squeeze()

    return result


# `agg` is an alias of `aggregate`.
agg = aggregate
register_base_override("agg")(agg)


# `_agg_helper` is not defined in modin, and used by Snowpark pandas to do extra validation.
@register_base_override("_agg_helper")
def _agg_helper(
    self,
    func: str,
    skipna: bool = True,
    axis: int | None | NoDefault = no_default,
    numeric_only: bool = False,
    **kwargs: Any,
):
    if not self._is_dataframe and numeric_only and not is_numeric_dtype(self.dtype):
        # Series aggregations on non-numeric data do not support numeric_only:
        # https://github.com/pandas-dev/pandas/blob/cece8c6579854f6b39b143e22c11cac56502c4fd/pandas/core/series.py#L6358
        raise TypeError(
            f"Series.{func} does not allow numeric_only=True with non-numeric dtypes."
        )
    axis = self._get_axis_number(axis)
    numeric_only = validate_bool_kwarg(numeric_only, "numeric_only", none_allowed=True)
    skipna = validate_bool_kwarg(skipna, "skipna", none_allowed=False)
    agg_kwargs: dict[str, Any] = {
        "numeric_only": numeric_only,
        "skipna": skipna,
    }
    agg_kwargs.update(kwargs)
    return self.aggregate(func=func, axis=axis, **agg_kwargs)


# See _agg_helper
@register_base_override("count")
def count(
    self,
    axis: Axis | None = 0,
    numeric_only: bool = False,
):
    """
    Count non-NA cells for `BasePandasDataset`.
    """
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    return self._agg_helper(
        func="count",
        axis=axis,
        numeric_only=numeric_only,
    )


# See _agg_helper
@register_base_override("max")
def max(
    self,
    axis: Axis | None = 0,
    skipna: bool = True,
    numeric_only: bool = False,
    **kwargs: Any,
):
    """
    Return the maximum of the values over the requested axis.
    """
    return self._agg_helper(
        func="max",
        axis=axis,
        skipna=skipna,
        numeric_only=numeric_only,
        **kwargs,
    )


# See _agg_helper
@register_base_override("min")
def min(
    self,
    axis: Axis | None | NoDefault = no_default,
    skipna: bool = True,
    numeric_only: bool = False,
    **kwargs,
):
    """
    Return the minimum of the values over the requested axis.
    """
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    return self._agg_helper(
        func="min",
        axis=axis,
        skipna=skipna,
        numeric_only=numeric_only,
        **kwargs,
    )


# See _agg_helper
@register_base_override("mean")
def mean(
    self,
    axis: Axis | None | NoDefault = no_default,
    skipna: bool = True,
    numeric_only: bool = False,
    **kwargs: Any,
):
    """
    Return the mean of the values over the requested axis.
    """
    return self._agg_helper(
        func="mean",
        axis=axis,
        skipna=skipna,
        numeric_only=numeric_only,
        **kwargs,
    )


# See _agg_helper
@register_base_override("median")
def median(
    self,
    axis: Axis | None | NoDefault = no_default,
    skipna: bool = True,
    numeric_only: bool = False,
    **kwargs: Any,
):
    """
    Return the mean of the values over the requested axis.
    """
    return self._agg_helper(
        func="median",
        axis=axis,
        skipna=skipna,
        numeric_only=numeric_only,
        **kwargs,
    )


# See _agg_helper
@register_base_override("std")
def std(
    self,
    axis: Axis | None = None,
    skipna: bool = True,
    ddof: int = 1,
    numeric_only: bool = False,
    **kwargs,
):
    """
    Return sample standard deviation over requested axis.
    """
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    kwargs.update({"ddof": ddof})
    return self._agg_helper(
        func="std",
        axis=axis,
        skipna=skipna,
        numeric_only=numeric_only,
        **kwargs,
    )


# See _agg_helper
@register_base_override("var")
def var(
    self,
    axis: Axis | None = None,
    skipna: bool = True,
    ddof: int = 1,
    numeric_only: bool = False,
    **kwargs: Any,
):
    """
    Return unbiased variance over requested axis.
    """
    kwargs.update({"ddof": ddof})
    return self._agg_helper(
        func="var",
        axis=axis,
        skipna=skipna,
        numeric_only=numeric_only,
        **kwargs,
    )


def _set_attrs(self, value: dict) -> None:  # noqa: RT01, D200
    # Use a field on the query compiler instead of self to avoid any possible ambiguity with
    # a column named "_attrs"
    self._query_compiler._attrs = copy.deepcopy(value)


def _get_attrs(self) -> dict:  # noqa: RT01, D200
    return self._query_compiler._attrs


register_base_override("attrs")(property(_get_attrs, _set_attrs))


@register_base_override("align")
def align(
    self,
    other: BasePandasDataset,
    join: str = "outer",
    axis: Axis = None,
    level: Level = None,
    copy: bool = True,
    fill_value: Scalar = None,
    method: str = None,
    limit: int = None,
    fill_axis: Axis = 0,
    broadcast_axis: Axis = None,
):  # noqa: PR01, RT01, D200
    from modin.pandas.dataframe import DataFrame

    if method is not None or limit is not None or fill_axis != 0:
        raise NotImplementedError(
            f"The 'method', 'limit', and 'fill_axis' keywords in {self.__class__.__name__}.align are deprecated and will be removed in a future version. Call fillna directly on the returned objects instead."
        )
    if broadcast_axis is not None:
        raise NotImplementedError(
            f"The 'broadcast_axis' keyword in {self.__class__.__name__}.align is deprecated and will be removed in a future version."
        )
    if axis not in [0, 1, None]:
        raise ValueError(
            f"No axis named {axis} for object type {self.__class__.__name__}"
        )
    if isinstance(self, Series) and axis == 1:
        raise ValueError("No axis named 1 for object type Series")

    is_lhs_dataframe_and_rhs_series = isinstance(self, pd.DataFrame) and isinstance(
        other, pd.Series
    )
    is_lhs_series_and_rhs_dataframe = isinstance(self, pd.Series) and isinstance(
        other, pd.DataFrame
    )

    if is_lhs_dataframe_and_rhs_series and axis is None:
        raise ValueError("Must specify axis=0 or 1")
    if (is_lhs_dataframe_and_rhs_series and axis == 1) or (
        is_lhs_series_and_rhs_dataframe and axis is None
    ):
        raise NotImplementedError(
            f"The Snowpark pandas {self.__class__.__name__}.align with {other.__class__.__name__} other does not "
            f"support axis={axis}."
        )

    query_compiler1, query_compiler2 = self._query_compiler.align(
        other, join=join, axis=axis, level=level, copy=copy, fill_value=fill_value
    )
    if is_lhs_dataframe_and_rhs_series:
        return DataFrame(query_compiler=query_compiler1), Series(
            query_compiler=query_compiler2
        )
    elif is_lhs_series_and_rhs_dataframe:
        return Series(query_compiler=query_compiler1), DataFrame(
            query_compiler=query_compiler2
        )
    else:
        return (
            self._create_or_update_from_compiler(query_compiler1, False),
            self._create_or_update_from_compiler(query_compiler2, False),
        )


# Modin does not provide `MultiIndex` support and will default to pandas when `level` is specified,
# and allows binary ops against native pandas objects that Snowpark pandas prohibits.
@register_base_override("_binary_op")
def _binary_op(
    self,
    op: str,
    other: BasePandasDataset,
    axis: Axis = None,
    level: Level | None = None,
    fill_value: float | None = None,
    **kwargs: Any,
):
    """
    Do binary operation between two datasets.

    Parameters
    ----------
    op : str
        Name of binary operation.
    other : modin.pandas.BasePandasDataset
        Second operand of binary operation.
    axis: Whether to compare by the index (0 or ‘index’) or columns. (1 or ‘columns’).
    level: Broadcast across a level, matching Index values on the passed MultiIndex level.
    fill_value: Fill existing missing (NaN) values, and any new element needed for
        successful DataFrame alignment, with this value before computation.
        If data in both corresponding DataFrame locations is missing the result will be missing.
        only arithmetic binary operation has this parameter (e.g., add() has, but eq() doesn't have).

    kwargs can contain the following parameters passed in at the frontend:
        func: Only used for `combine` method. Function that takes two series as inputs and
            return a Series or a scalar. Used to merge the two dataframes column by columns.

    Returns
    -------
    modin.pandas.BasePandasDataset
        Result of binary operation.
    """
    # In upstream modin, _axis indicates the operator will use the default axis
    if kwargs.pop("_axis", None) is None:
        if axis is not None:
            axis = self._get_axis_number(axis)
        else:
            axis = 1
    else:
        axis = 0
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    raise_if_native_pandas_objects(other)
    axis = self._get_axis_number(axis)
    squeeze_self = isinstance(self, pd.Series)

    # pandas itself will ignore the axis argument when using Series.<op>.
    # Per default, it is set to axis=0. However, for the case of a Series interacting with
    # a DataFrame the behavior is axis=1. Manually check here for this case and adjust the axis.

    is_lhs_series_and_rhs_dataframe = (
        True
        if isinstance(self, pd.Series) and isinstance(other, pd.DataFrame)
        else False
    )

    new_query_compiler = self._query_compiler.binary_op(
        op=op,
        other=other,
        axis=1 if is_lhs_series_and_rhs_dataframe else axis,
        level=level,
        fill_value=fill_value,
        squeeze_self=squeeze_self,
        **kwargs,
    )

    from modin.pandas.dataframe import DataFrame

    # Modin Bug: https://github.com/modin-project/modin/issues/7236
    # For a Series interacting with a DataFrame, always return a DataFrame
    return (
        DataFrame(query_compiler=new_query_compiler)
        if is_lhs_series_and_rhs_dataframe
        else self._create_or_update_from_compiler(new_query_compiler)
    )


# Current Modin does not use _dropna and instead defines `dropna` directly, but Snowpark pandas
# Series/DF still do. Snowpark pandas still needs to add support for the `ignore_index` parameter
# (added in pandas 2.0), and should be able to refactor to remove this override.
@register_base_override("_dropna")
def _dropna(
    self,
    axis: Axis = 0,
    how: str | NoDefault = no_default,
    thresh: int | NoDefault = no_default,
    subset: IndexLabel = None,
    inplace: bool = False,
):
    inplace = validate_bool_kwarg(inplace, "inplace")

    if is_list_like(axis):
        raise TypeError("supplying multiple axes to axis is no longer supported.")

    axis = self._get_axis_number(axis)

    if (how is not no_default) and (thresh is not no_default):
        raise TypeError(
            "You cannot set both the how and thresh arguments at the same time."
        )

    if how is no_default:
        how = "any"
    if how not in ["any", "all"]:
        raise ValueError("invalid how option: %s" % how)
    if subset is not None:
        if axis != 1:
            indices = self.columns.get_indexer_for(
                subset if is_list_like(subset) else [subset]
            )
            check = indices == -1
            if check.any():
                raise KeyError([k.item() for k in np.compress(check, subset)])

    new_query_compiler = self._query_compiler.dropna(
        axis=axis,
        how=how,
        thresh=thresh,
        subset=subset,
    )
    return self._create_or_update_from_compiler(new_query_compiler, inplace)


# Snowpark pandas uses `self_is_series` instead of `squeeze_self` and `squeeze_value` to determine
# the shape of `self` and `value`. Further work is needed to reconcile these two approaches.
@register_base_override("fillna")
def fillna(
    self,
    self_is_series,
    value: Hashable | Mapping | pd.Series | pd.DataFrame = None,
    method: FillnaOptions | None = None,
    axis: Axis | None = None,
    inplace: bool = False,
    limit: int | None = None,
    downcast: dict | None = None,
):
    """
    Fill NA/NaN values using the specified method.

    Parameters
    ----------
    self_is_series : bool
        If True then self contains a Series object, if False then self contains
        a DataFrame object.
    value : scalar, dict, Series, or DataFrame, default: None
        Value to use to fill holes (e.g. 0), alternately a
        dict/Series/DataFrame of values specifying which value to use for
        each index (for a Series) or column (for a DataFrame).  Values not
        in the dict/Series/DataFrame will not be filled. This value cannot
        be a list.
    method : {'backfill', 'bfill', 'pad', 'ffill', None}, default: None
        Method to use for filling holes in reindexed Series
        pad / ffill: propagate last valid observation forward to next valid
        backfill / bfill: use next valid observation to fill gap.
    axis : {None, 0, 1}, default: None
        Axis along which to fill missing values.
    inplace : bool, default: False
        If True, fill in-place. Note: this will modify any
        other views on this object (e.g., a no-copy slice for a column in a
        DataFrame).
    limit : int, default: None
        If method is specified, this is the maximum number of consecutive
        NaN values to forward/backward fill. In other words, if there is
        a gap with more than this number of consecutive NaNs, it will only
        be partially filled. If method is not specified, this is the
        maximum number of entries along the entire axis where NaNs will be
        filled. Must be greater than 0 if not None.
    downcast : dict, default: None
        A dict of item->dtype of what to downcast if possible,
        or the string 'infer' which will try to downcast to an appropriate
        equal type (e.g. float64 to int64 if possible).

    Returns
    -------
    Series, DataFrame or None
        Object with missing values filled or None if ``inplace=True``.
    """
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    raise_if_native_pandas_objects(value)
    inplace = validate_bool_kwarg(inplace, "inplace")
    axis = self._get_axis_number(axis)
    if isinstance(value, (list, tuple)):
        raise TypeError(
            '"value" parameter must be a scalar or dict, but '
            + f'you passed a "{type(value).__name__}"'
        )
    if value is None and method is None:
        # same as pandas
        raise ValueError("Must specify a fill 'value' or 'method'.")
    if value is not None and method is not None:
        raise ValueError("Cannot specify both 'value' and 'method'.")
    if method is not None and method not in ["backfill", "bfill", "pad", "ffill"]:
        expecting = "pad (ffill) or backfill (bfill)"
        msg = "Invalid fill method. Expecting {expecting}. Got {method}".format(
            expecting=expecting, method=method
        )
        raise ValueError(msg)
    if limit is not None:
        if not isinstance(limit, int):
            raise ValueError("Limit must be an integer")
        elif limit <= 0:
            raise ValueError("Limit must be greater than 0")

    new_query_compiler = self._query_compiler.fillna(
        self_is_series=self_is_series,
        value=value,
        method=method,
        axis=axis,
        limit=limit,
        downcast=downcast,
    )
    return self._create_or_update_from_compiler(new_query_compiler, inplace)


# Snowpark pandas passes the query compiler object from a BasePandasDataset, which Modin does not do.
@register_base_override("isin")
def isin(
    self, values: BasePandasDataset | ListLike | dict[Hashable, ListLike]
) -> BasePandasDataset:  # noqa: PR01, RT01, D200
    """
    Whether elements in `BasePandasDataset` are contained in `values`.
    """
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset

    # Pass as query compiler if values is BasePandasDataset.
    if isinstance(values, BasePandasDataset):
        values = values._query_compiler

    # Convert non-dict values to List if values is neither List[Any] nor np.ndarray. SnowflakeQueryCompiler
    # expects for the non-lazy case, where values is not a BasePandasDataset, the data to be materialized
    # as list or numpy array. Because numpy may perform implicit type conversions, use here list to be more general.
    elif not isinstance(values, dict) and (
        not isinstance(values, list) or not isinstance(values, np.ndarray)
    ):
        values = list(values)

    return self.__constructor__(query_compiler=self._query_compiler.isin(values=values))


# Snowpark pandas uses the single `quantiles_along_axis0` query compiler method, while upstream
# Modin splits this into `quantile_for_single_value` and `quantile_for_list_of_values` calls.
# It should be possible to merge those two functions upstream and reconcile the implementations.
@register_base_override("quantile")
def quantile(
    self,
    q: Scalar | ListLike = 0.5,
    axis: Axis = 0,
    numeric_only: bool = False,
    interpolation: Literal[
        "linear", "lower", "higher", "midpoint", "nearest"
    ] = "linear",
    method: Literal["single", "table"] = "single",
) -> float | BasePandasDataset:
    """
    Return values at the given quantile over requested axis.
    """
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    axis = self._get_axis_number(axis)

    # TODO
    # - SNOW-1008361: support axis=1
    # - SNOW-1008367: support when q is Snowpandas DF/Series (need to require QC interface to accept QC q values)
    # - SNOW-1003587: support datetime/timedelta columns

    if axis == 1 or interpolation not in ["linear", "nearest"] or method != "single":
        ErrorMessage.not_implemented(
            f"quantile function with parameters axis={axis}, interpolation={interpolation}, method={method} not supported"
        )

    if not numeric_only:
        # If not numeric_only and columns, then check all columns are either
        # numeric, timestamp, or timedelta
        # Check if dtype is numeric, timedelta ("m"), or datetime ("M")
        if not axis and not all(
            is_numeric_dtype(t) or lib.is_np_dtype(t, "mM") for t in self._get_dtypes()
        ):
            raise TypeError("can't multiply sequence by non-int of type 'float'")
        # If over rows, then make sure that all dtypes are equal for not
        # numeric_only
        elif axis:
            for i in range(1, len(self._get_dtypes())):
                pre_dtype = self._get_dtypes()[i - 1]
                curr_dtype = self._get_dtypes()[i]
                if not is_dtype_equal(pre_dtype, curr_dtype):
                    raise TypeError(
                        "Cannot compare type '{}' with type '{}'".format(
                            pre_dtype, curr_dtype
                        )
                    )
    else:
        # Normally pandas returns this near the end of the quantile, but we
        # can't afford the overhead of running the entire operation before
        # we error.
        if not any(is_numeric_dtype(t) for t in self._get_dtypes()):
            raise ValueError("need at least one array to concatenate")

    # check that all qs are between 0 and 1
    validate_percentile(q)
    axis = self._get_axis_number(axis)
    query_compiler = self._query_compiler.quantiles_along_axis0(
        q=q if is_list_like(q) else [q],
        numeric_only=numeric_only,
        interpolation=interpolation,
        method=method,
    )
    if is_list_like(q):
        return self.__constructor__(query_compiler=query_compiler)
    else:
        # result is either a scalar or Series
        result = self._reduce_dimension(query_compiler.transpose_single_row())
        if isinstance(result, BasePandasDataset):
            result.name = q
        return result


# Current Modin does not define this method. Snowpark pandas currently only uses it in
# `DataFrame.set_index`. Modin does not support MultiIndex, or have its own lazy index class,
# so we may need to keep this method for the foreseeable future.
@register_base_override("_to_series_list")
def _to_series_list(self, index: pd.Index) -> list[pd.Series]:
    """
    Convert index to a list of series
    Args:
        index: can be single or multi index

    Returns:
        the list of series
    """
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    if isinstance(index, pd.MultiIndex):
        return [
            pd.Series(index.get_level_values(level)) for level in range(index.nlevels)
        ]
    elif isinstance(index, pd.Index):
        return [pd.Series(index)]
    else:
        raise Exception("invalid index: " + str(index))


# Upstream modin defaults to pandas when `suffix` is provided.
@register_base_override("shift")
def shift(
    self,
    periods: int | Sequence[int] = 1,
    freq=None,
    axis: Axis = 0,
    fill_value: Hashable = no_default,
    suffix: str | None = None,
) -> BasePandasDataset:
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    if periods == 0 and freq is None:
        # Check obvious case first, freq manipulates the index even for periods == 0 so check for it in addition.
        return self.copy()

    # pandas compatible ValueError for freq='infer'
    # TODO: Test as part of SNOW-1023324.
    if freq == "infer":  # pragma: no cover
        if not hasattr(self, "freq") and not hasattr(  # pragma: no cover
            self, "inferred_freq"  # pragma: no cover
        ):  # pragma: no cover
            raise ValueError()  # pragma: no cover

    axis = self._get_axis_number(axis)

    if fill_value == no_default:
        fill_value = None

    new_query_compiler = self._query_compiler.shift(
        periods, freq, axis, fill_value, suffix
    )
    return self._create_or_update_from_compiler(new_query_compiler, False)


# Snowpark pandas supports only `numeric_only=True`, which is not the default value of the argument,
# so we have this overridden. We should revisit this behavior.
@register_base_override("skew")
def skew(
    self,
    axis: Axis | None | NoDefault = no_default,
    skipna: bool = True,
    numeric_only=True,
    **kwargs,
):  # noqa: PR01, RT01, D200
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    """
    Return unbiased skew over requested axis.
    """
    return self._stat_operation("skew", axis, skipna, numeric_only, **kwargs)


@register_base_override("resample")
def resample(
    self,
    rule,
    axis: Axis = lib.no_default,
    closed: str | None = None,
    label: str | None = None,
    convention: str = "start",
    kind: str | None = None,
    on: Level = None,
    level: Level = None,
    origin: str | TimestampConvertibleTypes = "start_day",
    offset: TimedeltaConvertibleTypes | None = None,
    group_keys=no_default,
):  # noqa: PR01, RT01, D200
    """
    Resample time-series data.
    """
    from snowflake.snowpark.modin.plugin.extensions.resample_overrides import Resampler

    if axis is not lib.no_default:  # pragma: no cover
        axis = self._get_axis_number(axis)
        if axis == 1:
            warnings.warn(
                "DataFrame.resample with axis=1 is deprecated. Do "
                + "`frame.T.resample(...)` without axis instead.",
                FutureWarning,
                stacklevel=1,
            )
        else:
            warnings.warn(
                f"The 'axis' keyword in {type(self).__name__}.resample is "
                + "deprecated and will be removed in a future version.",
                FutureWarning,
                stacklevel=1,
            )
    else:
        axis = 0

    return Resampler(
        dataframe=self,
        rule=rule,
        axis=axis,
        closed=closed,
        label=label,
        convention=convention,
        kind=kind,
        on=on,
        level=level,
        origin=origin,
        offset=offset,
        group_keys=group_keys,
    )


# Snowpark pandas needs to return a custom Expanding window object. We cannot use the
# extensions module for this at the moment because modin performs a relative import of
# `from .window import Expanding`.
@register_base_override("expanding")
def expanding(self, min_periods=1, axis=0, method="single"):  # noqa: PR01, RT01, D200
    """
    Provide expanding window calculations.
    """
    from snowflake.snowpark.modin.plugin.extensions.window_overrides import Expanding

    if axis is not lib.no_default:
        axis = self._get_axis_number(axis)
        name = "expanding"
        if axis == 1:
            warnings.warn(
                f"Support for axis=1 in {type(self).__name__}.{name} is "
                + "deprecated and will be removed in a future version. "
                + f"Use obj.T.{name}(...) instead",
                FutureWarning,
                stacklevel=1,
            )
        else:
            warnings.warn(
                f"The 'axis' keyword in {type(self).__name__}.{name} is "
                + "deprecated and will be removed in a future version. "
                + "Call the method without the axis keyword instead.",
                FutureWarning,
                stacklevel=1,
            )
    else:
        axis = 0

    return Expanding(
        self,
        min_periods=min_periods,
        axis=axis,
        method=method,
    )


# Same as Expanding: Snowpark pandas needs to return a custmo Window object.
@register_base_override("rolling")
def rolling(
    self,
    window,
    min_periods: int | None = None,
    center: bool = False,
    win_type: str | None = None,
    on: str | None = None,
    axis: Axis = lib.no_default,
    closed: str | None = None,
    step: int | None = None,
    method: str = "single",
):  # noqa: PR01, RT01, D200
    """
    Provide rolling window calculations.
    """
    if axis is not lib.no_default:
        axis = self._get_axis_number(axis)
        name = "rolling"
        if axis == 1:
            warnings.warn(
                f"Support for axis=1 in {type(self).__name__}.{name} is "
                + "deprecated and will be removed in a future version. "
                + f"Use obj.T.{name}(...) instead",
                FutureWarning,
                stacklevel=1,
            )
        else:  # pragma: no cover
            warnings.warn(
                f"The 'axis' keyword in {type(self).__name__}.{name} is "
                + "deprecated and will be removed in a future version. "
                + "Call the method without the axis keyword instead.",
                FutureWarning,
                stacklevel=1,
            )
    else:
        axis = 0

    if win_type is not None:
        from snowflake.snowpark.modin.plugin.extensions.window_overrides import Window

        return Window(
            self,
            window=window,
            min_periods=min_periods,
            center=center,
            win_type=win_type,
            on=on,
            axis=axis,
            closed=closed,
            step=step,
            method=method,
        )
    from snowflake.snowpark.modin.plugin.extensions.window_overrides import Rolling

    return Rolling(
        self,
        window=window,
        min_periods=min_periods,
        center=center,
        win_type=win_type,
        on=on,
        axis=axis,
        closed=closed,
        step=step,
        method=method,
    )


# Snowpark pandas uses a custom indexer object for all indexing methods.
@register_base_override("iloc")
@property
def iloc(self):
    """
    Purely integer-location based indexing for selection by position.
    """
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    # TODO: SNOW-930028 enable all skipped doctests
    from snowflake.snowpark.modin.plugin.extensions.indexing_overrides import (
        _iLocIndexer,
    )

    return _iLocIndexer(self)


# Snowpark pandas uses a custom indexer object for all indexing methods.
@register_base_override("loc")
@property
def loc(self):
    """
    Get a group of rows and columns by label(s) or a boolean array.
    """
    # TODO: SNOW-935444 fix doctest where index key has name
    # TODO: SNOW-933782 fix multiindex transpose bug, e.g., Name: (cobra, mark ii) => Name: ('cobra', 'mark ii')
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    from snowflake.snowpark.modin.plugin.extensions.indexing_overrides import (
        _LocIndexer,
    )

    return _LocIndexer(self)


# Snowpark pandas uses a custom indexer object for all indexing methods.
@register_base_override("iat")
@property
def iat(self, axis=None):  # noqa: PR01, RT01, D200
    """
    Get a single value for a row/column pair by integer position.
    """
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    from snowflake.snowpark.modin.plugin.extensions.indexing_overrides import (
        _iAtIndexer,
    )

    return _iAtIndexer(self)


# Snowpark pandas uses a custom indexer object for all indexing methods.
@register_base_override("at")
@property
def at(self, axis=None):  # noqa: PR01, RT01, D200
    """
    Get a single value for a row/column label pair.
    """
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    from snowflake.snowpark.modin.plugin.extensions.indexing_overrides import _AtIndexer

    return _AtIndexer(self)


# Snowpark pandas performs different dispatch logic; some changes may need to be upstreamed
# to fix edge case indexing behaviors.
@register_base_override("__getitem__")
def __getitem__(self, key):
    """
    Retrieve dataset according to `key`.

    Parameters
    ----------
    key : callable, scalar, slice, str or tuple
        The global row index to retrieve data from.

    Returns
    -------
    BasePandasDataset
        Located dataset.
    """
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    key = apply_if_callable(key, self)
    # If a slice is passed in, use .iloc[key].
    if isinstance(key, slice):
        if (is_integer(key.start) or key.start is None) and (
            is_integer(key.stop) or key.stop is None
        ):
            return self.iloc[key]
        else:
            return self.loc[key]

    # If the object calling getitem is a Series, only use .loc[key] to filter index.
    if isinstance(self, pd.Series):
        return self.loc[key]

    # Sometimes the result of a callable is a DataFrame (e.g. df[df > 0]) - use where.
    elif isinstance(key, pd.DataFrame):
        return self.where(cond=key)

    # If the object is a boolean list-like object, use .loc[key] to filter index.
    # The if statement is structured this way to avoid calling dtype and reduce query count.
    if isinstance(key, pd.Series):
        if pandas.api.types.is_bool_dtype(key.dtype):
            return self.loc[key]
    elif is_list_like(key):
        if hasattr(key, "dtype"):
            if pandas.api.types.is_bool_dtype(key.dtype):
                return self.loc[key]
        if (all(is_bool(k) for k in key)) and len(key) > 0:
            return self.loc[key]

    # In all other cases, use .loc[:, key] to filter columns.
    return self.loc[:, key]


# Modin uses the unique() query compiler method instead of aliasing the duplicated frontend method as of 0.30.1.
# TODO SNOW-1758721: use the more efficient implementation
@register_base_override("drop_duplicates")
def drop_duplicates(
    self, keep="first", inplace=False, **kwargs
):  # noqa: PR01, RT01, D200
    """
    Return `BasePandasDataset` with duplicate rows removed.
    """
    if keep not in ("first", "last", False):
        raise ValueError('keep must be either "first", "last" or False')
    inplace = validate_bool_kwarg(inplace, "inplace")
    ignore_index = kwargs.get("ignore_index", False)
    subset = kwargs.get("subset", None)
    if subset is not None:
        if is_list_like(subset):
            if not isinstance(subset, list):
                subset = list(subset)  # pragma: no cover
        else:
            subset = [subset]
        df = self[subset]
    else:
        df = self
    duplicated = df.duplicated(keep=keep)
    result = self[~duplicated]
    if ignore_index:
        result.index = pandas.RangeIndex(stop=len(result))
    if inplace:
        self._update_inplace(result._query_compiler)  # pragma: no cover
    else:
        return result


# Snowpark pandas does extra argument validation, which may need to be upstreamed.
@register_base_override("sort_values")
def sort_values(
    self,
    by,
    axis=0,
    ascending=True,
    inplace: bool = False,
    kind="quicksort",
    na_position="last",
    ignore_index: bool = False,
    key: IndexKeyFunc | None = None,
):  # noqa: PR01, RT01, D200
    """
    Sort by the values along either axis.
    """
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    axis = self._get_axis_number(axis)
    inplace = validate_bool_kwarg(inplace, "inplace")
    ascending = validate_ascending(ascending)
    if axis == 0:
        # If any column is None raise KeyError (same a native pandas).
        if by is None or (isinstance(by, list) and None in by):
            # Same error message as native pandas.
            raise KeyError(None)
        if not isinstance(by, list):
            by = [by]

        # Convert 'ascending' to sequence if needed.
        if not isinstance(ascending, Sequence):
            ascending = [ascending] * len(by)
        if len(by) != len(ascending):
            # Same error message as native pandas.
            raise ValueError(
                f"Length of ascending ({len(ascending)})"
                f" != length of by ({len(by)})"
            )

        columns = self._query_compiler.columns.values.tolist()
        index_names = self._query_compiler.get_index_names()
        for by_col in by:
            col_count = columns.count(by_col)
            index_count = index_names.count(by_col)
            if col_count == 0 and index_count == 0:
                # Same error message as native pandas.
                raise KeyError(by_col)
            if col_count and index_count:
                # Same error message as native pandas.
                raise ValueError(
                    f"'{by_col}' is both an index level and a column label, which is ambiguous."
                )
            if col_count > 1:
                # Same error message as native pandas.
                raise ValueError(f"The column label '{by_col}' is not unique.")

        if na_position not in get_args(NaPosition):
            # Same error message as native pandas for invalid 'na_position' value.
            raise ValueError(f"invalid na_position: {na_position}")
        result = self._query_compiler.sort_rows_by_column_values(
            by,
            ascending=ascending,
            kind=kind,
            na_position=na_position,
            ignore_index=ignore_index,
            key=key,
        )
    else:
        result = self._query_compiler.sort_columns_by_row_values(
            by,
            ascending=ascending,
            kind=kind,
            na_position=na_position,
            ignore_index=ignore_index,
            key=key,
        )
    return self._create_or_update_from_compiler(result, inplace)


# Modin does not define `where` on BasePandasDataset, and defaults to pandas at the frontend
# layer for Series.
@register_base_override("where")
def where(
    self,
    cond: BasePandasDataset | Callable | AnyArrayLike,
    other: BasePandasDataset | Callable | Scalar | None = np.nan,
    inplace: bool = False,
    axis: Axis | None = None,
    level: Level | None = None,
):
    """
    Replace values where the condition is False.
    """
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    # TODO: SNOW-985670: Refactor `where` and `mask`
    # will move pre-processing to QC layer.
    inplace = validate_bool_kwarg(inplace, "inplace")
    if cond is None:
        raise ValueError("Array conditional must be same shape as self")

    cond = apply_if_callable(cond, self)

    if isinstance(cond, Callable):
        raise NotImplementedError("Do not support callable for 'cond' parameter.")

    if isinstance(cond, Series):
        cond._query_compiler._shape_hint = "column"
    if isinstance(self, Series):
        self._query_compiler._shape_hint = "column"
    if isinstance(other, Series):
        other._query_compiler._shape_hint = "column"

    if not isinstance(cond, BasePandasDataset):
        cond = get_as_shape_compatible_dataframe_or_series(cond, self)
        cond._query_compiler._shape_hint = "array"

    if other is not None:
        other = apply_if_callable(other, self)

        if isinstance(other, np.ndarray):
            other = get_as_shape_compatible_dataframe_or_series(
                other,
                self,
                shape_mismatch_message="other must be the same shape as self when an ndarray",
            )
            other._query_compiler._shape_hint = "array"

        if isinstance(other, BasePandasDataset):
            other = other._query_compiler

    query_compiler = self._query_compiler.where(
        cond._query_compiler,
        other,
        axis,
        level,
    )

    return self._create_or_update_from_compiler(query_compiler, inplace)


# Snowpark pandas performs extra argument validation, some of which should be pushed down
# to the QC layer.
@register_base_override("mask")
def mask(
    self,
    cond: BasePandasDataset | Callable | AnyArrayLike,
    other: BasePandasDataset | Callable | Scalar | None = np.nan,
    inplace: bool = False,
    axis: Axis | None = None,
    level: Level | None = None,
):
    """
    Replace values where the condition is True.
    """
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    # TODO: https://snowflakecomputing.atlassian.net/browse/SNOW-985670
    # will move pre-processing to QC layer.
    inplace = validate_bool_kwarg(inplace, "inplace")
    if cond is None:
        raise ValueError("Array conditional must be same shape as self")

    cond = apply_if_callable(cond, self)

    if isinstance(cond, Callable):
        raise NotImplementedError("Do not support callable for 'cond' parameter.")

    if isinstance(cond, Series):
        cond._query_compiler._shape_hint = "column"
    if isinstance(self, Series):
        self._query_compiler._shape_hint = "column"
    if isinstance(other, Series):
        other._query_compiler._shape_hint = "column"

    if not isinstance(cond, BasePandasDataset):
        cond = get_as_shape_compatible_dataframe_or_series(cond, self)
        cond._query_compiler._shape_hint = "array"

    if other is not None:
        other = apply_if_callable(other, self)

        if isinstance(other, np.ndarray):
            other = get_as_shape_compatible_dataframe_or_series(
                other,
                self,
                shape_mismatch_message="other must be the same shape as self when an ndarray",
            )
            other._query_compiler._shape_hint = "array"

        if isinstance(other, BasePandasDataset):
            other = other._query_compiler

    query_compiler = self._query_compiler.mask(
        cond._query_compiler,
        other,
        axis,
        level,
    )

    return self._create_or_update_from_compiler(query_compiler, inplace)


# Snowpark pandas uses a custom I/O dispatcher class.
@register_base_override("to_csv")
def to_csv(
    self,
    path_or_buf=None,
    sep=",",
    na_rep="",
    float_format=None,
    columns=None,
    header=True,
    index=True,
    index_label=None,
    mode="w",
    encoding=None,
    compression="infer",
    quoting=None,
    quotechar='"',
    lineterminator=None,
    chunksize=None,
    date_format=None,
    doublequote=True,
    escapechar=None,
    decimal=".",
    errors: str = "strict",
    storage_options: StorageOptions = None,
):  # pragma: no cover
    from modin.core.execution.dispatching.factories.dispatcher import FactoryDispatcher

    return FactoryDispatcher.to_csv(
        self._query_compiler,
        path_or_buf=path_or_buf,
        sep=sep,
        na_rep=na_rep,
        float_format=float_format,
        columns=columns,
        header=header,
        index=index,
        index_label=index_label,
        mode=mode,
        encoding=encoding,
        compression=compression,
        quoting=quoting,
        quotechar=quotechar,
        lineterminator=lineterminator,
        chunksize=chunksize,
        date_format=date_format,
        doublequote=doublequote,
        escapechar=escapechar,
        decimal=decimal,
        errors=errors,
        storage_options=storage_options,
    )


# Modin has support for a custom NumPy wrapper module.
@register_base_override("to_numpy")
@materialization_warning
def to_numpy(
    self,
    dtype: npt.DTypeLike | None = None,
    copy: bool = False,
    na_value: object = no_default,
    **kwargs: Any,
) -> np.ndarray:
    """
    Convert the `BasePandasDataset` to a NumPy array or a Modin wrapper for NumPy array.
    """
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    if copy:
        WarningMessage.ignored_argument(
            operation="to_numpy",
            argument="copy",
            message="copy is ignored in Snowflake backend",
        )
    return self._query_compiler.to_numpy(
        dtype=dtype,
        na_value=na_value,
        **kwargs,
    )


# Modin performs extra argument validation and defaults to pandas for some edge cases.
@register_base_override("sample")
def sample(
    self,
    n: int | None = None,
    frac: float | None = None,
    replace: bool = False,
    weights: str | np.ndarray | None = None,
    random_state: RandomState | None = None,
    axis: Axis | None = None,
    ignore_index: bool = False,
):
    """
    Return a random sample of items from an axis of object.
    """
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    if self._get_axis_number(axis):
        if weights is not None and isinstance(weights, str):
            raise ValueError(
                "Strings can only be passed to weights when sampling from rows on a DataFrame"
            )
    else:
        if n is None and frac is None:
            n = 1
        elif n is not None and frac is not None:
            raise ValueError("Please enter a value for `frac` OR `n`, not both")
        else:
            if n is not None:
                if n < 0:
                    raise ValueError(
                        "A negative number of rows requested. Please provide `n` >= 0."
                    )
                if n % 1 != 0:
                    raise ValueError("Only integers accepted as `n` values")
            else:
                if frac < 0:
                    raise ValueError(
                        "A negative number of rows requested. Please provide `frac` >= 0."
                    )

    query_compiler = self._query_compiler.sample(
        n, frac, replace, weights, random_state, axis, ignore_index
    )
    return self.__constructor__(query_compiler=query_compiler)


# Modin performs an extra query calling self.isna() to raise a warning when fill_method is unspecified.
@register_base_override("pct_change")
def pct_change(
    self, periods=1, fill_method=no_default, limit=no_default, freq=None, **kwargs
):  # noqa: PR01, RT01, D200
    """
    Percentage change between the current and a prior element.
    """
    if fill_method not in (lib.no_default, None) or limit is not lib.no_default:
        warnings.warn(
            "The 'fill_method' keyword being not None and the 'limit' keyword in "
            + f"{type(self).__name__}.pct_change are deprecated and will be removed "
            + "in a future version. Either fill in any non-leading NA values prior "
            + "to calling pct_change or specify 'fill_method=None' to not fill NA "
            + "values.",
            FutureWarning,
            stacklevel=1,
        )
    if fill_method is lib.no_default:
        warnings.warn(
            f"The default fill_method='pad' in {type(self).__name__}.pct_change is "
            + "deprecated and will be removed in a future version. Either fill in any "
            + "non-leading NA values prior to calling pct_change or specify 'fill_method=None' "
            + "to not fill NA values.",
            FutureWarning,
            stacklevel=1,
        )
        fill_method = "pad"

    if limit is lib.no_default:
        limit = None

    kwargs["axis"] = self._get_axis_number(kwargs.get("axis", 0))

    # Attempting to match pandas error behavior here
    if not isinstance(periods, int):
        raise TypeError(f"periods must be an int. got {type(periods)} instead")

    column_is_timedelta_type = [
        self._query_compiler.is_timedelta64_dtype(i, is_index=False)
        for i in range(len(self._query_compiler.columns))
    ]

    if kwargs["axis"] == 1:
        if any(column_is_timedelta_type) and not all(column_is_timedelta_type):
            # pct_change() between timedelta and a non-timedelta type is invalid.
            raise TypeError(_TIMEDELTA_PCT_CHANGE_AXIS_1_MIXED_TYPE_ERROR_MESSAGE)

    # Attempting to match pandas error behavior here
    for i, dtype in enumerate(self._get_dtypes()):
        if not is_numeric_dtype(dtype) and not column_is_timedelta_type[i]:
            raise TypeError(
                f"cannot perform pct_change on non-numeric column with dtype {dtype}"
            )

    return self.__constructor__(
        query_compiler=self._query_compiler.pct_change(
            periods=periods,
            fill_method=fill_method,
            limit=limit,
            freq=freq,
            **kwargs,
        )
    )


# Snowpark pandas has different `copy` behavior, and some different behavior with native series arguments.
@register_base_override("astype")
def astype(
    self,
    dtype: str | type | pd.Series | dict[str, type],
    copy: bool = True,
    errors: Literal["raise", "ignore"] = "raise",
) -> pd.DataFrame | pd.Series:
    """
    Cast a Modin object to a specified dtype `dtype`.
    """
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    # dtype can be a series, a dict, or a scalar. If it's series or scalar,
    # convert it to a dict before passing it to the query compiler.
    raise_if_native_pandas_objects(dtype)

    if isinstance(dtype, Series):
        dtype = dtype.to_pandas()
        if not dtype.index.is_unique:
            raise ValueError(
                "The new Series of types must have a unique index, i.e. "
                + "it must be one-to-one mapping from column names to "
                + " their new dtypes."
            )
        dtype = dtype.to_dict()
    # If we got a series or dict originally, dtype is a dict now. Its keys
    # must be column names.
    if isinstance(dtype, dict):
        # Avoid materializing columns. The query compiler will handle errors where
        # dtype dict includes keys that are not in columns.
        col_dtypes = dtype
        for col_name in col_dtypes:
            if col_name not in self._query_compiler.columns:
                raise KeyError(
                    "Only a column name can be used for the key in a dtype mappings argument. "
                    f"'{col_name}' not found in columns."
                )
    else:
        # Assume that the dtype is a scalar.
        col_dtypes = {column: dtype for column in self._query_compiler.columns}

    # ensure values are pandas dtypes
    col_dtypes = {k: pandas_dtype(v) for k, v in col_dtypes.items()}
    new_query_compiler = self._query_compiler.astype(col_dtypes, errors=errors)
    return self._create_or_update_from_compiler(new_query_compiler, not copy)


# Modin defaults to pandsa when `level` is specified, and has some extra axis validation that
# is guarded in newer versions.
@register_base_override("drop")
def drop(
    self,
    labels: IndexLabel = None,
    axis: Axis = 0,
    index: IndexLabel = None,
    columns: IndexLabel = None,
    level: Level = None,
    inplace: bool = False,
    errors: IgnoreRaise = "raise",
) -> BasePandasDataset | None:
    """
    Drop specified labels from `BasePandasDataset`.
    """
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    inplace = validate_bool_kwarg(inplace, "inplace")
    if labels is not None:
        if index is not None or columns is not None:
            raise ValueError("Cannot specify both 'labels' and 'index'/'columns'")
        axes = {self._get_axis_number(axis): labels}
    elif index is not None or columns is not None:
        axes = {0: index, 1: columns}
    else:
        raise ValueError(
            "Need to specify at least one of 'labels', 'index' or 'columns'"
        )

    for axis, labels in axes.items():
        if labels is not None:
            if level is not None and not self._query_compiler.has_multiindex(axis=axis):
                # Same error as native pandas.
                raise AssertionError("axis must be a MultiIndex")
            # According to pandas documentation, a tuple will be used as a single
            # label and not treated as a list-like.
            if not is_list_like(labels) or isinstance(labels, tuple):
                axes[axis] = [labels]

    new_query_compiler = self._query_compiler.drop(
        index=axes.get(0), columns=axes.get(1), level=level, errors=errors
    )
    return self._create_or_update_from_compiler(new_query_compiler, inplace)


# Modin calls len(self.index) instead of a direct query compiler method.
@register_base_override("__len__")
def __len__(self) -> int:
    """
    Return length of info axis.

    Returns
    -------
    int
    """
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    return self._query_compiler.get_axis_len(axis=0)


# Snowpark pandas ignores `copy`.
@register_base_override("set_axis")
def set_axis(
    self,
    labels: IndexLabel,
    *,
    axis: Axis = 0,
    copy: bool | NoDefault = no_default,
):
    """
    Assign desired index to given axis.
    """
    # Behavior based on copy:
    # -----------------------------------
    # - In native pandas, copy determines whether to create a copy of the data (not DataFrame).
    # - We cannot emulate the native pandas' copy behavior in Snowpark since a copy of only data
    #   cannot be created -- you can only copy the whole object (DataFrame/Series).
    #
    # Snowpark behavior:
    # ------------------
    # - copy is kept for compatibility with native pandas but is ignored. The user is warned that copy is unused.
    # Warn user that copy does not do anything.
    if copy is not no_default:
        WarningMessage.single_warning(
            message=f"{type(self).__name__}.set_axis 'copy' keyword is unused and is ignored."
        )
    if labels is None:
        raise TypeError("None is not a valid value for the parameter 'labels'.")

    # Determine whether to update self or a copy and perform update.
    obj = self.copy()
    setattr(obj, axis, labels)
    return obj


# Modin has different behavior for empty dataframes and some slightly different length validation.
@register_base_override("describe")
def describe(
    self,
    percentiles: ListLike | None = None,
    include: ListLike | Literal["all"] | None = None,
    exclude: ListLike | None = None,
) -> BasePandasDataset:
    """
    Generate descriptive statistics.
    """
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    percentiles = _refine_percentiles(percentiles)
    data = self
    if self._is_dataframe:
        # Upstream modin lacks this check because it defaults to pandas for describing empty dataframes
        if len(self.columns) == 0:
            raise ValueError("Cannot describe a DataFrame without columns")

        # include/exclude are ignored for Series
        if (include is None) and (exclude is None):
            # when some numerics are found, keep only numerics
            default_include: list[npt.DTypeLike] = [np.number]
            default_include.append("datetime")
            data = self.select_dtypes(include=default_include)
            if len(data.columns) == 0:
                data = self
        elif include == "all":
            if exclude is not None:
                raise ValueError("exclude must be None when include is 'all'")
            data = self
        else:
            data = self.select_dtypes(
                include=include,
                exclude=exclude,
            )
    # Upstream modin uses data.empty, but that incurs an extra row count query
    if self._is_dataframe and len(data.columns) == 0:
        # Match pandas error from concatenating empty list of series descriptions.
        raise ValueError("No objects to concatenate")

    return self.__constructor__(
        query_compiler=data._query_compiler.describe(percentiles=percentiles)
    )


# Modin does type validation on self that Snowpark pandas defers to SQL.
@register_base_override("diff")
def diff(self, periods: int = 1, axis: Axis = 0):
    """
    First discrete difference of element.
    """
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    # We must only accept integer (or float values that are whole numbers)
    # for periods.
    int_periods = validate_int_kwarg(periods, "periods", float_allowed=True)
    axis = self._get_axis_number(axis)
    return self.__constructor__(
        query_compiler=self._query_compiler.diff(axis=axis, periods=int_periods)
    )


# Modin does an unnecessary len call when n == 0.
@register_base_override("tail")
def tail(self, n: int = 5):
    if n == 0:
        return self.iloc[0:0]
    return self.iloc[-n:]


# Snowpark pandas does extra argument validation (which should probably be deferred to SQL instead).
@register_base_override("idxmax")
def idxmax(self, axis=0, skipna=True, numeric_only=False):  # noqa: PR01, RT01, D200
    """
    Return index of first occurrence of maximum over requested axis.
    """
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    dtypes = self._get_dtypes()
    if (
        axis == 1
        and not numeric_only
        and any(not is_numeric_dtype(d) for d in dtypes)
        and len(set(dtypes)) > 1
    ):
        # For numeric_only=False, if we have any non-numeric dtype, e.g.
        # a string type, we need every other column to be of the same type.
        # We can't compare two objects of different non-numeric types, e.g.
        # a string and a timestamp.
        # If we have only numeric data, we can compare columns even if they
        # different types, e.g. we can compare an int column to a float
        # column.
        raise TypeError("'>' not supported for these dtypes")
    axis = self._get_axis_number(axis)
    return self._reduce_dimension(
        self._query_compiler.idxmax(axis=axis, skipna=skipna, numeric_only=numeric_only)
    )


# Snowpark pandas does extra argument validation (which should probably be deferred to SQL instead).
@register_base_override("idxmin")
def idxmin(self, axis=0, skipna=True, numeric_only=False):  # noqa: PR01, RT01, D200
    """
    Return index of first occurrence of minimum over requested axis.
    """
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    dtypes = self._get_dtypes()
    if (
        axis == 1
        and not numeric_only
        and any(not is_numeric_dtype(d) for d in dtypes)
        and len(set(dtypes)) > 1
    ):
        # For numeric_only=False, if we have any non-numeric dtype, e.g.
        # a string type, we need every other column to be of the same type.
        # We can't compare two objects of different non-numeric types, e.g.
        # a string and a timestamp.
        # If we have only numeric data, we can compare columns even if they
        # different types, e.g. we can compare an int column to a float
        # column.
        raise TypeError("'<' not supported for these dtypes")
    axis = self._get_axis_number(axis)
    return self._reduce_dimension(
        self._query_compiler.idxmin(axis=axis, skipna=skipna, numeric_only=numeric_only)
    )


# Modin does dtype validation on unary ops that Snowpark pandas does not.
@register_base_override("__abs__")
def abs(self):  # noqa: RT01, D200
    """
    Return a `BasePandasDataset` with absolute numeric value of each element.
    """
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    return self.__constructor__(query_compiler=self._query_compiler.abs())


# Modin does dtype validation on unary ops that Snowpark pandas does not.
@register_base_override("__invert__")
def __invert__(self):
    """
    Apply bitwise inverse to each element of the `BasePandasDataset`.

    Returns
    -------
    BasePandasDataset
        New BasePandasDataset containing bitwise inverse to each value.
    """
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    return self.__constructor__(query_compiler=self._query_compiler.invert())


# Modin does dtype validation on unary ops that Snowpark pandas does not.
@register_base_override("__neg__")
def __neg__(self):
    """
    Change the sign for every value of self.

    Returns
    -------
    BasePandasDataset
    """
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    return self.__constructor__(query_compiler=self._query_compiler.negative())


# Modin needs to add a check for mapper is not None, which changes query counts in test_concat.py
# if not present.
@register_base_override("rename_axis")
def rename_axis(
    self,
    mapper=lib.no_default,
    *,
    index=lib.no_default,
    columns=lib.no_default,
    axis=0,
    copy=None,
    inplace=False,
):  # noqa: PR01, RT01, D200
    """
    Set the name of the axis for the index or columns.
    """
    axes = {"index": index, "columns": columns}

    if copy is None:
        copy = True

    if axis is not None:
        axis = self._get_axis_number(axis)

    inplace = validate_bool_kwarg(inplace, "inplace")

    if mapper is not lib.no_default and mapper is not None:
        # Use v0.23 behavior if a scalar or list
        non_mapper = is_scalar(mapper) or (
            is_list_like(mapper) and not is_dict_like(mapper)
        )
        if non_mapper:
            return self._set_axis_name(mapper, axis=axis, inplace=inplace)
        else:
            raise ValueError("Use `.rename` to alter labels with a mapper.")
    else:
        # Use new behavior.  Means that index and/or columns is specified
        result = self if inplace else self.copy(deep=copy)

        for axis in range(self.ndim):
            v = axes.get(pandas.DataFrame._get_axis_name(axis))
            if v is lib.no_default:
                continue
            non_mapper = is_scalar(v) or (is_list_like(v) and not is_dict_like(v))
            if non_mapper:
                newnames = v
            else:

                def _get_rename_function(mapper):
                    if isinstance(mapper, (dict, BasePandasDataset)):

                        def f(x):
                            if x in mapper:
                                return mapper[x]
                            else:
                                return x

                    else:
                        f = mapper

                    return f

                f = _get_rename_function(v)
                curnames = self.index.names if axis == 0 else self.columns.names
                newnames = [f(name) for name in curnames]
            result._set_axis_name(newnames, axis=axis, inplace=True)
        if not inplace:
            return result


# Snowpark pandas has custom dispatch logic for ufuncs, while modin defaults to pandas.
@register_base_override("__array_ufunc__")
def __array_ufunc__(self, ufunc: np.ufunc, method: str, *inputs, **kwargs):
    """
    Apply the `ufunc` to the `BasePandasDataset`.

    Parameters
    ----------
    ufunc : np.ufunc
        The NumPy ufunc to apply.
    method : str
        The method to apply.
    *inputs : tuple
        The inputs to the ufunc.
    **kwargs : dict
        Additional keyword arguments.

    Returns
    -------
    BasePandasDataset
        The result of the ufunc applied to the `BasePandasDataset`.
    """
    # Use pandas version of ufunc if it exists
    if method != "__call__":
        # Return sentinel value NotImplemented
        return NotImplemented  # pragma: no cover
    from snowflake.snowpark.modin.plugin.utils.numpy_to_pandas import (
        numpy_to_pandas_universal_func_map,
    )

    if ufunc.__name__ in numpy_to_pandas_universal_func_map:
        ufunc = numpy_to_pandas_universal_func_map[ufunc.__name__]
        if ufunc == NotImplemented:
            return NotImplemented
        # We cannot support the out argument
        if kwargs.get("out") is not None:
            return NotImplemented
        return ufunc(self, inputs[1:])
    # return the sentinel NotImplemented if we do not support this function
    return NotImplemented  # pragma: no cover


# Snowpark pandas does extra argument validation.
@register_base_override("reindex")
def reindex(
    self,
    index=None,
    columns=None,
    copy=True,
    **kwargs,
):  # noqa: PR01, RT01, D200
    """
    Conform `BasePandasDataset` to new index with optional filling logic.
    """
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    if kwargs.get("limit", None) is not None and kwargs.get("method", None) is None:
        raise ValueError(
            "limit argument only valid if doing pad, backfill or nearest reindexing"
        )
    new_query_compiler = None
    if index is not None:
        if not isinstance(index, pandas.Index) or not index.equals(self.index):
            new_query_compiler = self._query_compiler.reindex(
                axis=0, labels=index, **kwargs
            )
    if new_query_compiler is None:
        new_query_compiler = self._query_compiler
    final_query_compiler = None
    if columns is not None:
        if not isinstance(index, pandas.Index) or not columns.equals(self.columns):
            final_query_compiler = new_query_compiler.reindex(
                axis=1, labels=columns, **kwargs
            )
    if final_query_compiler is None:
        final_query_compiler = new_query_compiler
    return self._create_or_update_from_compiler(
        final_query_compiler, inplace=False if copy is None else not copy
    )


# No direct override annotation; used as part of `property`.
# Snowpark pandas may return a custom lazy index object.
def _get_index(self):
    """
    Get the index for this DataFrame.

    Returns
    -------
    pandas.Index
        The union of all indexes across the partitions.
    """
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    from snowflake.snowpark.modin.plugin.extensions.index import Index

    if self._query_compiler.is_multiindex():
        # Lazy multiindex is not supported
        return self._query_compiler.index

    idx = Index(query_compiler=self._query_compiler)
    idx._set_parent(self)
    return idx


# No direct override annotation; used as part of `property`.
# Snowpark pandas may return a custom lazy index object.
def _set_index(self, new_index: Axes) -> None:
    """
    Set the index for this DataFrame.

    Parameters
    ----------
    new_index : pandas.Index
        The new index to set this.
    """
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    self._update_inplace(
        new_query_compiler=self._query_compiler.set_index(
            [s._query_compiler for s in self._to_series_list(ensure_index(new_index))]
        )
    )


# Snowpark pandas may return a custom lazy index object.
register_base_override("index")(property(_get_index, _set_index))
