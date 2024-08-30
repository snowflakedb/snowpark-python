#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

"""
File containing Series APIs defined in the Modin API layer, but with different behavior in Snowpark
pandas, such as `Series.memory_usage`.
"""

from __future__ import annotations

from typing import Any

import pandas as native_pd
from modin.pandas import Series
from pandas._libs.lib import no_default

from snowflake.snowpark.modin import pandas as spd  # noqa: F401
from snowflake.snowpark.modin.pandas.api.extensions import register_series_accessor
from snowflake.snowpark.modin.plugin._internal.telemetry import (
    snowpark_pandas_telemetry_method_decorator,
)
from snowflake.snowpark.modin.plugin._typing import ListLike
from snowflake.snowpark.modin.plugin.utils.error_message import series_not_implemented
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage
from snowflake.snowpark.modin.utils import _inherit_docstrings


def register_series_not_implemented():
    def decorator(base_method: Any):
        func = snowpark_pandas_telemetry_method_decorator(
            series_not_implemented()(base_method)
        )
        register_series_accessor(base_method.__name__)(func)
        return func

    return decorator


# === UNIMPLEMENTED METHODS ===
# The following methods are not implemented in Snowpark pandas, and must be overridden on the
# frontend. These methods fall into a few categories:
# 1. Would work in Snowpark pandas, but we have not tested it.
# 2. Would work in Snowpark pandas, but requires more SQL queries than we are comfortable with.
# 3. Requires materialization (usually via a frontend _default_to_pandas call).
# 4. Performs operations on a native pandas Index object that are nontrivial for Snowpark pandas to manage.


@register_series_not_implemented()
def argsort(self, axis=0, kind="quicksort", order=None):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_series_not_implemented()
def transform(self, func, axis=0, *args, **kwargs):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_series_not_implemented()
def autocorr(self, lag=1):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_series_not_implemented()
def between(self, left, right, inclusive: str = "both"):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_series_not_implemented()
def corr(self, other, method="pearson", min_periods=None):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_series_not_implemented()
def cov(self, other, min_periods=None, ddof: int | None = 1):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_series_not_implemented()
def divmod(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_series_not_implemented()
def dot(self, other):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_series_not_implemented()
def explode(self, ignore_index: bool = False):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_series_not_implemented()
def factorize(
    self, sort=False, na_sentinel=no_default, use_na_sentinel=no_default
):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_series_not_implemented()
def hist(
    self,
    by=None,
    ax=None,
    grid=True,
    xlabelsize=None,
    xrot=None,
    ylabelsize=None,
    yrot=None,
    figsize=None,
    bins=10,
    **kwds,
):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_series_not_implemented()
def interpolate(
    self,
    method="linear",
    axis=0,
    limit=None,
    inplace=False,
    limit_direction: str | None = None,
    limit_area=None,
    downcast=None,
    **kwargs,
):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_series_not_implemented()
def item(self):  # noqa: RT01, D200
    pass  # pragma: no cover


@register_series_not_implemented()
def items(self):  # noqa: RT01, D200
    pass  # pragma: no cover


@register_series_not_implemented()
def prod(
    self,
    axis=None,
    skipna=True,
    level=None,
    numeric_only=False,
    min_count=0,
    **kwargs,
):
    pass  # pragma: no cover


@register_series_not_implemented()
def ravel(self, order="C"):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_series_not_implemented()
def reindex_like(
    self,
    other,
    method=None,
    copy: bool | None = None,
    limit=None,
    tolerance=None,
) -> Series:
    pass  # pragma: no cover


@register_series_not_implemented()
def reorder_levels(self, order):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_series_not_implemented()
def searchsorted(self, value, side="left", sorter=None):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_series_not_implemented()
def swaplevel(self, i=-2, j=-1, copy=True):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_series_not_implemented()
def to_period(self, freq=None, copy=True):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_series_not_implemented()
def to_string(
    self,
    buf=None,
    na_rep="NaN",
    float_format=None,
    header=True,
    index=True,
    length=False,
    dtype=False,
    name=False,
    max_rows=None,
    min_rows=None,
):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_series_not_implemented()
def to_timestamp(self, freq=None, how="start", copy=True):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_series_not_implemented()
def array(self):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_series_not_implemented()
def nbytes(self):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_series_not_implemented()
def __reduce__(self):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


# === OVERRIDDEN METHODS ===
# The below methods have their frontend implementations overridden compared to the version present
# in series.py. This is usually for one of the following reasons:
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


# Since Snowpark pandas leaves all data on the warehouse, memory_usage's report of local memory
# usage isn't meaningful and is set to always return 0.
@_inherit_docstrings(native_pd.Series.memory_usage, apilink="pandas.Series")
@register_series_accessor("memory_usage")
@snowpark_pandas_telemetry_method_decorator()
def memory_usage(self, index: bool = True, deep: bool = False) -> int:
    """
    Return zero bytes for memory_usage
    """
    # TODO: SNOW-1264697: push implementation down to query compiler
    return 0


# Snowpark pandas has slightly different type validation from upstream modin.
@_inherit_docstrings(native_pd.Series.isin, apilink="pandas.Series")
@register_series_accessor("isin")
@snowpark_pandas_telemetry_method_decorator()
def isin(self, values: set | ListLike) -> Series:
    """
    Whether elements in Series are contained in `values`.

    Return a boolean Series showing whether each element in the Series
    matches an element in the passed sequence of `values`.

    Caution
    -------
    Snowpark pandas deviates from pandas here with respect to NA values: when the value is considered NA or
    values contains at least one NA, None is returned instead of a boolean value.

    Parameters
    ----------
    values : set or list-like
        The sequence of values to test. Passing in a single string will
        raise a ``TypeError``. Instead, turn a single string into a
        list of one element.

    Returns
    -------
    Series
        Series of booleans indicating if each element is in values.

    Examples
    --------
    >>> s = pd.Series(['lama', 'cow', 'lama', 'beetle', 'lama',
    ...                'hippo'], name='animal')
    >>> s.isin(['cow', 'lama'])
    0     True
    1     True
    2     True
    3    False
    4     True
    5    False
    Name: animal, dtype: bool

    To invert the boolean values, use the ``~`` operator:

    >>> ~s.isin(['cow', 'lama'])
    0    False
    1    False
    2    False
    3     True
    4    False
    5     True
    Name: animal, dtype: bool

    Passing a single string as ``s.isin('lama')`` will raise an error. Use
    a list of one element instead:

    >>> s.isin(['lama'])
    0     True
    1    False
    2     True
    3    False
    4     True
    5    False
    Name: animal, dtype: bool

    >>> pd.Series([1]).isin(['1'])
    0    False
    dtype: bool

    >>> pd.Series([1, 2, None]).isin([2])
    0    False
    1     True
    2     None
    dtype: object
    """

    # pandas compatible TypeError
    if isinstance(values, str):
        raise TypeError(
            "only list-like objects are allowed to be passed to isin(), you passed a [str]"
        )

    # convert to list if given as set
    if isinstance(values, set):
        values = list(values)

    return super(Series, self).isin(values)


# Snowpark pandas raises a warning before materializing data and passing to `plot`.
@register_series_accessor("plot")
@property
@snowpark_pandas_telemetry_method_decorator
def plot(
    self,
    kind="line",
    ax=None,
    figsize=None,
    use_index=True,
    title=None,
    grid=None,
    legend=False,
    style=None,
    logx=False,
    logy=False,
    loglog=False,
    xticks=None,
    yticks=None,
    xlim=None,
    ylim=None,
    rot=None,
    fontsize=None,
    colormap=None,
    table=False,
    yerr=None,
    xerr=None,
    label=None,
    secondary_y=False,
    **kwds,
):  # noqa: PR01, RT01, D200
    """
    Make plot of Series.
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    WarningMessage.single_warning(
        "Series.plot materializes data to the local machine for plotting."
    )
    return self._to_pandas().plot


# modin 0.28.1 doesn't define type annotations on properties, so we override this
# to satisfy test_type_annotations.py
_old_empty_fget = Series.empty.fget


@register_series_accessor("empty")
@property
@snowpark_pandas_telemetry_method_decorator()
def empty(self) -> bool:
    return _old_empty_fget(self)


# Upstream modin performs name change and copy operations on binary operators that Snowpark
# pandas avoids.
# Don't put telemetry on this method.
@register_series_accessor("_prepare_inter_op")
def _prepare_inter_op(self, other):
    # override prevents extra queries from occurring during binary operations
    return self, other
