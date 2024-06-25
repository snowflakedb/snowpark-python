#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

"""
File containing Series APIs defined in the Modin API layer, but with different behavior in Snowpark
pandas, such as `Series.memory_usage`.
"""

from __future__ import annotations

import pandas as native_pd
from modin.pandas import Series

from snowflake.snowpark.modin import pandas as pd  # noqa: F401
from snowflake.snowpark.modin.pandas.api.extensions import register_series_accessor
from snowflake.snowpark.modin.plugin._typing import ListLike
from snowflake.snowpark.modin.plugin.utils.error_message import series_not_implemented
from snowflake.snowpark.modin.utils import _inherit_docstrings

# These methods are not implemented by Snowpark pandas and raise errors at the frontend layer
frontend_not_implemented = [
    "argmax",
    "argmin",
    "argsort",
    "array",
    "asfreq",
    "asof",
    "at",
    "at_time",
    "autocorr",
    "backfill",
    "between",
    "between_time",
    "bfill",
    "bool",
    "clip",
    "combine",
    "combine_first",
    "compare",
    "divmod",
    "dot",
    "droplevel",
    "ewm",
    "explode",
    "factorize",
    "filter",
    "hist",
    "infer_objects",
    "interpolate",
    "item",
    "kurt",
    "kurtosis",
    "mode",
    "nbytes",
    "nlargest",
    "nsmallest",
    "nsmallest",
    "pipe",
    "plot",
    "pop",
    "prod",
    "ravel",
    "reindex_like",
    "reorder_levels",
    "repeat",
    "rdivmod",
    "searchsorted",
    "sem",
    "set_flags",
    "swapaxes",
    "swaplevel",
    "to_clipboard",
    "to_csv",
    "to_excel",
    "to_hdf",
    "to_json",
    "to_latex",
    "to_markdown",
    "to_period",
    "to_sql",
    "to_string",
    "to_timestamp",
    "to_xarray",
    "transform",
    "truncate",
    "tz_convert",
    "tz_localize",
    "unstack",
    "view",
    "xs",
]

for name in frontend_not_implemented:
    register_series_accessor(name)(series_not_implemented()(getattr(pd.Series, name)))


@_inherit_docstrings(native_pd.Series.memory_usage, apilink="pandas.Series")
@register_series_accessor("memory_usage")
def memory_usage(self, index: bool = True, deep: bool = False) -> int:
    """
    Return zero bytes for memory_usage
    """
    # TODO: SNOW-1264697: push implementation down to query compiler
    return 0


@_inherit_docstrings(native_pd.Series.isin, apilink="pandas.Series")
@register_series_accessor("isin")
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


# modin 0.28.1 doesn't define type annotations on properties, so we override this
# to satisfy test_type_annotations.py
_old_empty_fget = Series.empty.fget


@register_series_accessor("empty")
@property
def empty(self) -> bool:
    return _old_empty_fget(self)


@register_series_accessor("_prepare_inter_op")
def _prepare_inter_op(self, other):
    # override prevents extra queries from occurring during binary operations
    return self, other
