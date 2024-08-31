#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

"""
File containing Series APIs defined in the Modin API layer, but with different behavior in Snowpark
pandas, such as `Series.memory_usage`.
"""

from __future__ import annotations

from typing import Any

import modin.pandas as pd
import pandas as native_pd
from modin.pandas import Series
from modin.pandas.base import BasePandasDataset
from pandas._libs.lib import NoDefault, is_integer, no_default
from pandas._typing import Axis, IndexLabel
from pandas.core.common import apply_if_callable, is_bool_indexer
from pandas.core.dtypes.common import is_bool_dtype, is_list_like

from snowflake.snowpark.modin import pandas as spd  # noqa: F401
from snowflake.snowpark.modin.pandas.api.extensions import register_series_accessor
from snowflake.snowpark.modin.pandas.utils import (
    from_pandas,
    is_scalar,
    try_convert_index_to_native,
)
from snowflake.snowpark.modin.plugin._internal.telemetry import (
    snowpark_pandas_telemetry_method_decorator,
)
from snowflake.snowpark.modin.plugin._typing import ListLike
from snowflake.snowpark.modin.plugin.utils.error_message import (
    ErrorMessage,
    series_not_implemented,
)
from snowflake.snowpark.modin.plugin.utils.frontend_constants import (
    SERIES_SETITEM_INCOMPATIBLE_INDEXER_WITH_SCALAR_ERROR_MESSAGE,
    SERIES_SETITEM_INCOMPATIBLE_INDEXER_WITH_SERIES_ERROR_MESSAGE,
    SERIES_SETITEM_LIST_LIKE_KEY_AND_RANGE_LIKE_VALUE_ERROR_MESSAGE,
    SERIES_SETITEM_SLICE_AS_SCALAR_VALUE_ERROR_MESSAGE,
)
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage
from snowflake.snowpark.modin.utils import (
    MODIN_UNNAMED_SERIES_LABEL,
    _inherit_docstrings,
)


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


# Snowpark pandas overrides the constructor for two reasons:
# 1. To support the Snowpark pandas lazy index object
# 2. To avoid raising "UserWarning: Distributing <class 'list'> object. This may take some time."
#    when a literal is passed in as data.
@register_series_accessor("__init__")
def __init__(
    self,
    data=None,
    index=None,
    dtype=None,
    name=None,
    copy=False,
    fastpath=False,
    query_compiler=None,
) -> None:
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    # Siblings are other dataframes that share the same query compiler. We
    # use this list to update inplace when there is a shallow copy.
    self._siblings = []

    # modified:
    # Engine.subscribe(_update_engine)

    # Convert lazy index to Series without pulling the data to client.
    if isinstance(data, pd.Index):
        query_compiler = data.to_series(index=index, name=name)._query_compiler
        query_compiler = query_compiler.reset_index(drop=True)
    elif isinstance(data, type(self)):
        query_compiler = data._query_compiler.copy()
        if index is not None:
            if any(i not in data.index for i in index):
                ErrorMessage.not_implemented(
                    "Passing non-existent columns or index values to constructor "
                    + "not yet implemented."
                )  # pragma: no cover
            query_compiler = data.loc[index]._query_compiler
    if query_compiler is None:
        # Defaulting to pandas
        if name is None:
            name = MODIN_UNNAMED_SERIES_LABEL
            if (
                isinstance(data, (native_pd.Series, native_pd.Index, pd.Index))
                and data.name is not None
            ):
                name = data.name

        query_compiler = from_pandas(
            native_pd.DataFrame(
                native_pd.Series(
                    data=try_convert_index_to_native(data),
                    index=try_convert_index_to_native(index),
                    dtype=dtype,
                    name=name,
                    copy=copy,
                    fastpath=fastpath,
                )
            )
        )._query_compiler
    self._query_compiler = query_compiler.columnarize()
    if name is not None:
        self.name = name


# Since Snowpark pandas leaves all data on the warehouse, memory_usage's report of local memory
# usage isn't meaningful and is set to always return 0.
@_inherit_docstrings(native_pd.Series.memory_usage, apilink="pandas.Series")
@register_series_accessor("memory_usage")
@snowpark_pandas_telemetry_method_decorator
def memory_usage(self, index: bool = True, deep: bool = False) -> int:
    """
    Return zero bytes for memory_usage
    """
    # TODO: SNOW-1264697: push implementation down to query compiler
    return 0


# Snowpark pandas has slightly different type validation from upstream modin.
@_inherit_docstrings(native_pd.Series.isin, apilink="pandas.Series")
@register_series_accessor("isin")
@snowpark_pandas_telemetry_method_decorator
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


# Upstream Modin has a bug binary operators (except add/radd, ) don't respect fill_value:
# https://github.com/modin-project/modin/issues/7381
@register_series_accessor("sub")
@snowpark_pandas_telemetry_method_decorator
def sub(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
    """
    Return subtraction of Series and `other`, element-wise (binary operator `sub`).
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).sub(other, level=level, fill_value=fill_value, axis=axis)


subtract = register_series_accessor("subtract")(sub)


@register_series_accessor("rsub")
@snowpark_pandas_telemetry_method_decorator
def rsub(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
    """
    Return subtraction of series and `other`, element-wise (binary operator `rsub`).
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).rsub(
        other, level=level, fill_value=fill_value, axis=axis
    )


@register_series_accessor("mul")
@snowpark_pandas_telemetry_method_decorator
def mul(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
    """
    Return multiplication of series and `other`, element-wise (binary operator `mul`).
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).mul(other, level=level, fill_value=fill_value, axis=axis)


multiply = register_series_accessor("multiply")(mul)


@register_series_accessor("rmul")
@snowpark_pandas_telemetry_method_decorator
def rmul(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
    """
    Return multiplication of series and `other`, element-wise (binary operator `mul`).
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).rmul(
        other, level=level, fill_value=fill_value, axis=axis
    )


@register_series_accessor("truediv")
@snowpark_pandas_telemetry_method_decorator
def truediv(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
    """
    Return floating division of series and `other`, element-wise (binary operator `truediv`).
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).truediv(
        other, level=level, fill_value=fill_value, axis=axis
    )


div = register_series_accessor("div")(truediv)
divide = register_series_accessor("divide")(truediv)


@register_series_accessor("rtruediv")
@snowpark_pandas_telemetry_method_decorator
def rtruediv(
    self, other, level=None, fill_value=None, axis=0
):  # noqa: PR01, RT01, D200
    """
    Return floating division of series and `other`, element-wise (binary operator `rtruediv`).
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).rtruediv(
        other, level=level, fill_value=fill_value, axis=axis
    )


rdiv = register_series_accessor("rdiv")(rtruediv)


@register_series_accessor("floordiv")
@snowpark_pandas_telemetry_method_decorator
def floordiv(
    self, other, level=None, fill_value=None, axis=0
):  # noqa: PR01, RT01, D200
    """
    Get Integer division of dataframe and `other`, element-wise (binary operator `floordiv`).
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).floordiv(
        other, level=level, fill_value=fill_value, axis=axis
    )


@register_series_accessor("rfloordiv")
@snowpark_pandas_telemetry_method_decorator
def rfloordiv(
    self, other, level=None, fill_value=None, axis=0
):  # noqa: PR01, RT01, D200
    """
    Return integer division of series and `other`, element-wise (binary operator `rfloordiv`).
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).rfloordiv(
        other, level=level, fill_value=fill_value, axis=axis
    )


@register_series_accessor("mod")
@snowpark_pandas_telemetry_method_decorator
def mod(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
    """
    Return Modulo of series and `other`, element-wise (binary operator `mod`).
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).mod(other, level=level, fill_value=fill_value, axis=axis)


@register_series_accessor("rmod")
@snowpark_pandas_telemetry_method_decorator
def rmod(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
    """
    Return modulo of series and `other`, element-wise (binary operator `rmod`).
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).rmod(
        other, level=level, fill_value=fill_value, axis=axis
    )


@register_series_accessor("pow")
@snowpark_pandas_telemetry_method_decorator
def pow(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
    """
    Return exponential power of series and `other`, element-wise (binary operator `pow`).
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).pow(other, level=level, fill_value=fill_value, axis=axis)


@register_series_accessor("rpow")
@snowpark_pandas_telemetry_method_decorator
def rpow(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
    """
    Return exponential power of series and `other`, element-wise (binary operator `rpow`).
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).rpow(
        other, level=level, fill_value=fill_value, axis=axis
    )


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__add__")
@snowpark_pandas_telemetry_method_decorator
def __add__(self, right):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return self.add(right)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__radd__")
@snowpark_pandas_telemetry_method_decorator
def __radd__(self, left):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return self.radd(left)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__and__")
@snowpark_pandas_telemetry_method_decorator
def __and__(self, other):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).__and__(other)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__rand__")
@snowpark_pandas_telemetry_method_decorator
def __rand__(self, other):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).__rand__(other)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__divmod__")
@snowpark_pandas_telemetry_method_decorator
def __divmod__(self, right):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return self.divmod(right)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__rdivmod__")
@snowpark_pandas_telemetry_method_decorator
def __rdivmod__(self, left):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return self.rdivmod(left)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__floordiv__")
@snowpark_pandas_telemetry_method_decorator
def __floordiv__(self, right):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return self.floordiv(right)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__rfloordiv__")
@snowpark_pandas_telemetry_method_decorator
def __rfloordiv__(self, right):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return self.rfloordiv(right)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__mod__")
@snowpark_pandas_telemetry_method_decorator
def __mod__(self, right):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return self.mod(right)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__rmod__")
@snowpark_pandas_telemetry_method_decorator
def __rmod__(self, left):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return self.rmod(left)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__mul__")
@snowpark_pandas_telemetry_method_decorator
def __mul__(self, right):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return self.mul(right)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__rmul__")
@snowpark_pandas_telemetry_method_decorator
def __rmul__(self, left):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return self.rmul(left)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__or__")
@snowpark_pandas_telemetry_method_decorator
def __or__(self, other):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).__or__(other)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__ror__")
@snowpark_pandas_telemetry_method_decorator
def __ror__(self, other):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).__ror__(other)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__xor__")
@snowpark_pandas_telemetry_method_decorator
def __xor__(self, other):  # pragma: no cover
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).__xor__(other)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__rxor__")
@snowpark_pandas_telemetry_method_decorator
def __rxor__(self, other):  # pragma: no cover
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).__rxor__(other)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__pow__")
@snowpark_pandas_telemetry_method_decorator
def __pow__(self, right):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return self.pow(right)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__rpow__")
@snowpark_pandas_telemetry_method_decorator
def __rpow__(self, left):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return self.rpow(left)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__sub__")
@snowpark_pandas_telemetry_method_decorator
def __sub__(self, right):
    return self.sub(right)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__rsub__")
@snowpark_pandas_telemetry_method_decorator
def __rsub__(self, left):
    return self.rsub(left)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__truediv__")
@snowpark_pandas_telemetry_method_decorator
def __truediv__(self, right):
    return self.truediv(right)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__rtruediv__")
@snowpark_pandas_telemetry_method_decorator
def __rtruediv__(self, left):
    return self.rtruediv(left)


register_series_accessor("__iadd__")(__add__)
# In upstream modin, imul is typo'd to be __add__ instead of __mul__
register_series_accessor("__imul__")(__mul__)
register_series_accessor("__ipow__")(__pow__)
register_series_accessor("__isub__")(__sub__)
register_series_accessor("__itruediv__")(__truediv__)


# Snowpark pandas does not yet support Categorical types. Return a dummy object instead of immediately
# erroring out so we get error messages describing which method a user tried to access.
class CategoryMethods:
    category_not_supported_message = "CategoricalDType and corresponding methods is not available in Snowpark pandas API yet!"

    def __init__(self, series) -> None:
        self._series = series
        self._query_compiler = series._query_compiler

    @property
    def categories(self):
        ErrorMessage.not_implemented(self.category_not_supported_message)

    @categories.setter
    def categories(self, categories):
        ErrorMessage.not_implemented(
            self.category_not_supported_message
        )  # pragma: no cover

    @property
    def ordered(self):
        ErrorMessage.not_implemented(self.category_not_supported_message)

    @property
    def codes(self):
        ErrorMessage.not_implemented(self.category_not_supported_message)

    def rename_categories(self, new_categories, inplace=False):
        ErrorMessage.not_implemented(self.category_not_supported_message)

    def reorder_categories(self, new_categories, ordered=None, inplace=False):
        ErrorMessage.not_implemented(self.category_not_supported_message)

    def add_categories(self, new_categories, inplace=False):
        ErrorMessage.not_implemented(self.category_not_supported_message)

    def remove_categories(self, removals, inplace=False):
        ErrorMessage.not_implemented(self.category_not_supported_message)

    def remove_unused_categories(self, inplace=False):
        ErrorMessage.not_implemented(self.category_not_supported_message)

    def set_categories(self, new_categories, ordered=None, rename=False, inplace=False):
        ErrorMessage.not_implemented(self.category_not_supported_message)

    def as_ordered(self, inplace=False):
        ErrorMessage.not_implemented(self.category_not_supported_message)

    def as_unordered(self, inplace=False):
        ErrorMessage.not_implemented(self.category_not_supported_message)


@register_series_accessor("cat")
@property
@snowpark_pandas_telemetry_method_decorator
def cat(self) -> CategoryMethods:
    return CategoryMethods(self)


# modin 0.28.1 doesn't define type annotations on properties, so we override this
# to satisfy test_type_annotations.py
_old_empty_fget = Series.empty.fget


@register_series_accessor("empty")
@property
@snowpark_pandas_telemetry_method_decorator
def empty(self) -> bool:
    return _old_empty_fget(self)


# Snowpark pandas defines a custom GroupBy object
@register_series_accessor("groupby")
@property
@snowpark_pandas_telemetry_method_decorator
def groupby(
    self,
    by=None,
    axis: Axis = 0,
    level: IndexLabel | None = None,
    as_index: bool = True,
    sort: bool = True,
    group_keys: bool = True,
    observed: bool | NoDefault = no_default,
    dropna: bool = True,
):
    """
    Group Series using a mapper or by a Series of columns.
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    from snowflake.snowpark.modin.pandas.groupby import (
        SeriesGroupBy,
        validate_groupby_args,
    )

    validate_groupby_args(by, level, observed)

    if not as_index:
        raise TypeError("as_index=False only valid with DataFrame")

    axis = self._get_axis_number(axis)
    return SeriesGroupBy(
        self,
        by,
        axis,
        level,
        as_index,
        sort,
        group_keys,
        idx_name=None,
        observed=observed,
        dropna=dropna,
    )


# Upstream Modin performs name change and copy operations on binary operators that Snowpark
# pandas avoids.
# Don't put telemetry on this method since it's an internal helper.
@register_series_accessor("_prepare_inter_op")
def _prepare_inter_op(self, other):
    # override prevents extra queries from occurring during binary operations
    return self, other


# Upstream Modin has a single _to_datetime QC method for both Series and DF, while Snowpark
# pandas distinguishes between the two.
# Don't put telemetry on this method since it's an internal helper.
@register_series_accessor("_to_datetime")
def _to_datetime(self, **kwargs):
    """
    Convert `self` to datetime.

    Parameters
    ----------
    **kwargs : dict
        Optional arguments to use during query compiler's
        `to_datetime` invocation.

    Returns
    -------
    datetime
        Series of datetime64 dtype.
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return self.__constructor__(
        query_compiler=self._query_compiler.series_to_datetime(**kwargs)
    )


# Snowpark pandas has the extra `statement_params` argument.
@register_series_accessor("_to_pandas")
@snowpark_pandas_telemetry_method_decorator
def _to_pandas(
    self,
    *,
    statement_params: dict[str, str] | None = None,
    **kwargs: Any,
):
    """
    Convert Snowpark pandas Series to pandas Series

    Args:
        statement_params: Dictionary of statement level parameters to be set while executing this action.

    Returns:
        pandas series
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    df = self._query_compiler.to_pandas(statement_params=statement_params, **kwargs)
    if len(df.columns) == 0:
        return native_pd.Series([])
    series = df[df.columns[0]]
    # special case when series is wrapped as dataframe, but has not label.
    # This is indicated with MODIN_UNNAMED_SERIES_LABEL
    if self._query_compiler.columns[0] == MODIN_UNNAMED_SERIES_LABEL:
        series.name = None

    return series


# Snowpark pandas does more validation and error checking than upstream Modin.
@register_series_accessor("__setitem__")
@snowpark_pandas_telemetry_method_decorator
def __setitem__(self, key, value):
    """
    Set `value` identified by `key` in the Series.

    Parameters
    ----------
    key : hashable
        Key to set.
    value : Any
        Value to set.

    Examples
    --------
    Using the following series to set values on. __setitem__ is an inplace operation, so copies of `series`are made
    in the examples to highlight the different behaviors produced.
    >>> series = pd.Series([1, "b", 3], index=["a", "b", "c"])

    Using a scalar as the value to set a particular element.
    >>> s = series.copy()
    >>> s["c"] = "a"
    >>> s
    a    1
    b    b
    c    a
    dtype: object

    Using list-like objects as the key and value to set multiple elements.
    >>> s = series.copy()
    >>> s[["c", "a"]] = ["foo", "bar"]
    >>> s  # doctest: +SKIP
    a    bar
    b      2
    c    foo
    dtype: object

    Having a duplicate label in the key.
    >>> s = series.copy()
    >>> s[["c", "a", "c"]] = pd.Index(["foo", "bar", "baz"])
    >>> s  # doctest: +SKIP
    a    bar
    b      2
    c    baz
    dtype: object

    When using a Series as the value, its index does not matter.
    >>> s = series.copy()  # doctest: +SKIP
    >>> s[["a", "b"]] = pd.Series([9, 8], index=["foo", "bar"])
    >>> s  # doctest: +SKIP
    a    9
    b    8
    c    3
    dtype: int64
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    key = apply_if_callable(key, self)

    # Error Checking:
    # Currently do not support Series[scalar key] = Series item/DataFrame item since this results in a nested series
    # or df.
    if is_scalar(key) and isinstance(value, BasePandasDataset):
        raise ValueError(
            SERIES_SETITEM_INCOMPATIBLE_INDEXER_WITH_SCALAR_ERROR_MESSAGE.format(
                "Snowpark pandas " + value.__class__.__name__
                if isinstance(value, BasePandasDataset)
                else value.__class__.__name__
            )
        )
    if isinstance(key, pd.DataFrame):
        raise ValueError(SERIES_SETITEM_INCOMPATIBLE_INDEXER_WITH_SERIES_ERROR_MESSAGE)
    elif (isinstance(key, pd.Series) or is_list_like(key)) and (
        isinstance(value, range)
    ):
        raise NotImplementedError(
            SERIES_SETITEM_LIST_LIKE_KEY_AND_RANGE_LIKE_VALUE_ERROR_MESSAGE
        )
    elif isinstance(value, slice):
        # Here, the whole slice is assigned as a scalar variable, i.e., a spot at an index gets a slice value.
        raise NotImplementedError(SERIES_SETITEM_SLICE_AS_SCALAR_VALUE_ERROR_MESSAGE)

    if isinstance(key, (slice, range)):
        if (key.start is None or is_integer(key.start)) and (  # pragma: no cover
            key.stop is None or is_integer(key.stop)
        ):
            # integer slice behaves the same as iloc slice
            self.iloc[key] = value  # pragma: no cover
        else:
            # TODO: SNOW-976232 once the slice test is added to test_setitem, code here should be covered.
            self.loc[key] = value  # pragma: no cover

    elif isinstance(value, Series):
        # If value is a Series, value's index doesn't matter/is ignored. However, loc setitem matches the key's
        # index with value's index. To emulate this behavior, treat the Series as if it is matching by position.
        #
        # For example,
        # With __setitem__, the index of value does not matter.
        # >>> series = pd.Series([1, 2, 3], index=["a", "b", "c"])
        # >>> series[["a", "b"]] = pd.Series([9, 8])
        # a    9
        # b    8
        # c    3
        # dtype: int64
        # value = pd.Series([9, 8], index=["foo", "bar"]) also produces same result as above.
        #
        # However, with loc setitem, index matters.
        # >>> series.loc[["a", "b"]] = pd.Series([9, 8])
        # a    NaN
        # b    NaN
        # c    3.0
        # dtype: float64
        #
        # >>> series.loc[["a", "b"]] = pd.Series([9, 8], index=["a", "b"])
        # a    9
        # b    8
        # c    3
        # dtype: int64
        # Due to the behavior above, loc setitem can work with any kind of value regardless of length.
        # With __setitem__, the length of the value must match length of the key. Currently, loc setitem can
        # handle this with boolean keys.

        # Convert list-like keys to Series.
        if not isinstance(key, pd.Series) and is_list_like(key):
            key = pd.Series(key)

        index_is_bool_indexer = False

        if isinstance(key, pd.Series) and is_bool_dtype(key.dtype):
            index_is_bool_indexer = True  # pragma: no cover
        elif is_bool_indexer(key):
            index_is_bool_indexer = True  # pragma: no cover

        new_qc = self._query_compiler.set_2d_labels(
            key._query_compiler if isinstance(key, BasePandasDataset) else key,
            slice(None),  # column key is not applicable to Series objects
            value._query_compiler,
            matching_item_columns_by_label=False,
            matching_item_rows_by_label=False,
            index_is_bool_indexer=index_is_bool_indexer,
        )
        self._update_inplace(new_query_compiler=new_qc)

    else:
        self.loc[key] = value


# Snowpark pandas uses the query compiler build_repr_df method to minimize queries, while upstream
# modin calls BasePandasDataset.build_repr_df
@register_series_accessor("__repr__")
@snowpark_pandas_telemetry_method_decorator
def __repr__(self):
    """
    Return a string representation for a particular Series.

    Returns
    -------
    str
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    num_rows = native_pd.get_option("display.max_rows") or 60
    num_cols = native_pd.get_option("display.max_columns") or 20

    (
        row_count,
        col_count,
        temp_df,
    ) = self._query_compiler.build_repr_df(num_rows, num_cols)
    if isinstance(temp_df, native_pd.DataFrame) and not temp_df.empty:
        temp_df = temp_df.iloc[:, 0]
    temp_str = repr(temp_df)
    freq_str = (
        f"Freq: {temp_df.index.freqstr}, "
        if isinstance(temp_df.index, native_pd.DatetimeIndex)
        else ""
    )
    if self.name is not None:
        name_str = f"Name: {str(self.name)}, "
    else:
        name_str = ""
    if row_count > num_rows:
        len_str = f"Length: {row_count}, "
    else:
        len_str = ""
    dtype_str = "dtype: {}".format(
        str(self.dtype) + ")" if temp_df.empty else temp_str.rsplit("dtype: ", 1)[-1]
    )
    if row_count == 0:
        return f"Series([], {freq_str}{name_str}{dtype_str}"
    maxsplit = 1
    if (
        isinstance(temp_df, native_pd.Series)
        and temp_df.name is not None
        and temp_df.dtype == "category"
    ):
        maxsplit = 2
    return temp_str.rsplit("\n", maxsplit)[0] + "\n{}{}{}{}".format(
        freq_str, name_str, len_str, dtype_str
    )
