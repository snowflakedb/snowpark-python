#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""
File containing Series APIs defined in the Modin API layer, but with different behavior in Snowpark
pandas, such as `Series.memory_usage`.
"""

from __future__ import annotations

import copy
import functools
from typing import IO, Any, Callable, Hashable, Literal, Mapping, Sequence, get_args

import modin.pandas as pd
import numpy as np
import numpy.typing as npt
import pandas as native_pd
from modin.pandas import DataFrame, Series
from modin.pandas.base import (
    BasePandasDataset,
    _ATTRS_NO_LOOKUP,
    sentinel,
)
from modin.core.storage_formats.pandas.query_compiler_caster import (
    EXTENSION_NO_LOOKUP,
    register_function_for_pre_op_switch,
)
from modin.pandas.io import from_pandas
from modin.pandas.utils import is_scalar
from pandas._libs.lib import NoDefault, is_integer, no_default
from pandas._typing import (
    AggFuncType,
    AnyArrayLike,
    ArrayLike,
    Axis,
    FilePath,
    FillnaOptions,
    IgnoreRaise,
    IndexKeyFunc,
    IndexLabel,
    Level,
    NaPosition,
    Renamer,
    Scalar,
    StorageOptions,
    WriteExcelBuffer,
)
from pandas.core.common import apply_if_callable, is_bool_indexer
from pandas.core.dtypes.common import is_bool_dtype, is_dict_like, is_list_like
from pandas.util._validators import validate_ascending, validate_bool_kwarg

from snowflake.snowpark.modin.plugin._internal.utils import (
    assert_fields_are_none,
    convert_index_to_list_of_qcs,
    convert_index_to_qc,
    error_checking_for_init,
)
from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    HYBRID_SWITCH_FOR_UNIMPLEMENTED_METHODS,
)
from snowflake.snowpark.modin.plugin._typing import DropKeep, ListLike
from snowflake.snowpark.modin.plugin.extensions.snow_partition_iterator import (
    SnowparkPandasRowPartitionIterator,
)
from snowflake.snowpark.modin.plugin.utils.error_message import (
    ErrorMessage,
    series_not_implemented,
)
from snowflake.snowpark.modin.plugin.utils.frontend_constants import (
    SERIES_ITEMS_WARNING_MESSAGE,
    SERIES_SETITEM_INCOMPATIBLE_INDEXER_WITH_SCALAR_ERROR_MESSAGE,
    SERIES_SETITEM_INCOMPATIBLE_INDEXER_WITH_SERIES_ERROR_MESSAGE,
    SERIES_SETITEM_LIST_LIKE_KEY_AND_RANGE_LIKE_VALUE_ERROR_MESSAGE,
    SERIES_SETITEM_SLICE_AS_SCALAR_VALUE_ERROR_MESSAGE,
)
from snowflake.snowpark.modin.plugin.utils.warning_message import (
    WarningMessage,
    materialization_warning,
)
from snowflake.snowpark.modin.utils import (
    MODIN_UNNAMED_SERIES_LABEL,
    _inherit_docstrings,
    validate_int_kwarg,
)

from modin.pandas.api.extensions import (
    register_series_accessor as _register_series_accessor,
)

register_series_accessor = functools.partial(
    _register_series_accessor, backend="Snowflake"
)


def register_series_not_implemented():
    def decorator(base_method: Any):
        func = series_not_implemented()(base_method)
        name = (
            base_method.fget.__name__
            if isinstance(base_method, property)
            else base_method.__name__
        )
        HYBRID_SWITCH_FOR_UNIMPLEMENTED_METHODS.add(("Series", name))
        register_function_for_pre_op_switch(
            class_name="Series", backend="Snowflake", method=name
        )
        register_series_accessor(name)(func)
        return func

    return decorator


# Upstream modin has an extra check for `key in self.index`, which produces an extra query
# when an attribute is not present.
# Because __getattr__ itself is responsible for resolving extension methods, we cannot override
# this method via the extensions module, and have to do it with an old-fashioned set.
# We cannot name this method __getattr__ because Python will treat this as this file's __getattr__.
def _getattr_impl(self, key):
    """
    Return item identified by `key`.
    Parameters
    ----------
    key : hashable
        Key to get.
    Returns
    -------
    Any
    Notes
    -----
    First try to use `__getattribute__` method. If it fails
    try to get `key` from `Series` fields.
    """
    # NOTE that to get an attribute, python calls __getattribute__() first and
    # then falls back to __getattr__() if the former raises an AttributeError.
    try:
        if key not in EXTENSION_NO_LOOKUP:
            extension = self._getattr__from_extension_impl(
                key, set(), Series._extensions
            )
            if extension is not sentinel:
                return extension
        return super(Series, self).__getattr__(key)
    except AttributeError as err:
        if key not in _ATTRS_NO_LOOKUP:
            try:
                value = self[key]
                if isinstance(value, Series) and value.empty:
                    raise err
                return value
            except Exception:
                # We want to raise err if self[key] raises any kind of exception
                raise err
        raise err


Series.__getattr__ = _getattr_impl


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


@register_series_accessor("hist")
def hist(
    self,
    by=None,
    ax=None,
    grid: bool = True,
    xlabelsize: int | None = None,
    xrot: float | None = None,
    ylabelsize: int | None = None,
    yrot: float | None = None,
    figsize: tuple[int, int] | None = None,
    bins: int | Sequence[int] = 10,
    backend: str | None = None,
    legend: bool = False,
    **kwargs,
):  # noqa: PR01, RT01, D200
    if bins is None:
        bins = 10

    # Get the query compiler representing the histogram data to be plotted.
    # Along with the query compiler, also get the minimum and maximum values in the input series, and the computed bin size.
    (
        new_query_compiler,
        min_val,
        max_val,
        bin_size,
    ) = self._query_compiler.hist_on_series(
        by=by,
        xlabelsize=xlabelsize,
        xrot=xrot,
        ylabelsize=ylabelsize,
        yrot=yrot,
        figsize=figsize,
        bins=bins,
        backend=backend,
        legend=legend,
        **kwargs,
    )

    # Convert the result to native pandas in preparation for plotting it using Matplotlib's bar chart.
    # Note that before converting to native pandas, the data had already been reduced in the previous step.
    native_ser = self.__constructor__(query_compiler=new_query_compiler)._to_pandas()

    # Ensure that we have enough rows in the series corresponding to all bins, even if some of them are empty.
    native_ser_reindexed = native_ser.reindex(
        native_pd.Index(np.linspace(min_val, max_val, bins + 1)),
        method="nearest",
        tolerance=bin_size / 3,
    ).fillna(0)

    # Prepare the visualization parameters to be used for rendering the bar chart.
    import matplotlib.pyplot as plt

    fig = kwargs.pop(
        "figure", plt.gcf() if plt.get_fignums() else plt.figure(figsize=figsize)
    )
    if ax is None:
        ax = fig.gca()
    ax.grid(grid)
    counts = native_ser_reindexed.to_list()[0:-1]
    vals = native_ser_reindexed.index.to_list()[0:-1]
    bar_labels = vals
    ax.bar(vals, counts, label=bar_labels, width=bin_size, align="edge")
    return ax


@register_series_not_implemented()
def item(self):  # noqa: RT01, D200
    pass  # pragma: no cover


# Snowpark pandas has a custom iterator.
@register_series_accessor("items")
def items(self):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    """
    Iterate over ``Series`` rows as (index, value) tuples.
    """

    def items_builder(s):
        """Return tuple of the given ``Series`` in the form (index, value)."""
        return s.name, s.squeeze()

    # Raise warning message since Series.items is very inefficient.
    WarningMessage.single_warning(SERIES_ITEMS_WARNING_MESSAGE)

    return SnowparkPandasRowPartitionIterator(DataFrame(self), items_builder, True)


@register_series_not_implemented()
def mode(self, dropna=True):  # noqa: PR01, RT01, D200
    pass


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
def rdivmod(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
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
def repeat(self, repeats, axis=None):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_series_not_implemented()
def searchsorted(self, value, side="left", sorter=None):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_series_not_implemented()
def swaplevel(self, i=-2, j=-1, copy=True):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


def to_excel(
    self,
    excel_writer: FilePath | WriteExcelBuffer | pd.ExcelWriter,
    sheet_name: str = "Sheet1",
    na_rep: str = "",
    float_format: str | None = None,
    columns: Sequence[Hashable] | None = None,
    header: Sequence[Hashable] | bool = True,
    index: bool = True,
    index_label: IndexLabel | None = None,
    startrow: int = 0,
    startcol: int = 0,
    engine: Literal["openpyxl", "xlsxwriter"] | None = None,
    merge_cells: bool = True,
    inf_rep: str = "inf",
    freeze_panes: tuple[int, int] | None = None,
    storage_options: StorageOptions | None = None,
    engine_kwargs: dict[str, Any] | None = None,
):  # noqa: PR01, RT01, D200
    WarningMessage.single_warning(
        "Series.to_excel materializes data to the local machine."
    )
    return self._to_pandas().to_excel(
        excel_writer=excel_writer,
        sheet_name=sheet_name,
        na_rep=na_rep,
        float_format=float_format,
        columns=columns,
        header=header,
        index=index,
        index_label=index_label,
        startrow=startrow,
        startcol=startcol,
        engine=engine,
        merge_cells=merge_cells,
        inf_rep=inf_rep,
        freeze_panes=freeze_panes,
        storage_options=storage_options,
        engine_kwargs=engine_kwargs,
    )


@register_series_not_implemented()
def to_period(self, freq=None, copy=True):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


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
    WarningMessage.single_warning(
        "Series.to_string materializes data to the local machine."
    )
    return self._to_pandas().to_string(
        buf=buf,
        na_rep=na_rep,
        float_format=float_format,
        header=header,
        index=index,
        length=length,
        dtype=dtype,
        name=name,
        max_rows=max_rows,
        min_rows=min_rows,
    )


@register_series_not_implemented()
def to_timestamp(self, freq=None, how="start", copy=True):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_series_not_implemented()
def update(self, other) -> None:  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_series_not_implemented()
def view(self, dtype=None):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_series_not_implemented()
@property
def array(self):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_series_not_implemented()
@property
def nbytes(self):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_series_not_implemented()
def sem(
    self,
    axis: Axis | None = None,
    skipna: bool = True,
    ddof: int = 1,
    numeric_only=False,
    **kwargs,
):  # noqa: PR01, RT01, D200
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

    from snowflake.snowpark.modin.plugin.extensions.index import Index

    # Setting the query compiler
    # --------------------------
    if query_compiler is not None:
        # If a query_compiler is passed in, only use the query_compiler and name fields to create a new Series.
        # Verify that the data and index parameters are None.
        assert_fields_are_none(class_name="Series", data=data, index=index, dtype=dtype)
        self._query_compiler = query_compiler.columnarize()
        if name is not None:
            self.name = name
        return

    # A DataFrame cannot be used as an index and Snowpark pandas does not support the Categorical type yet.
    # Check that index is not a DataFrame and dtype is not "category".
    error_checking_for_init(index, dtype)

    if isinstance(data, pd.DataFrame):
        # data cannot be a DataFrame, raise a clear error message.
        # pandas raises an ambiguous error:
        # ValueError: The truth value of a DataFrame is ambiguous. Use a.empty, a.bool(), a.item(), a.any() or a.all().
        raise ValueError("Data cannot be a DataFrame")

    # The logic followed here is:
    # STEP 1: Create a query_compiler from the provided data.
    # STEP 2: If an index is provided, set the index. This is either through set_index or reindex.
    # STEP 3: If a dtype is given, and it is different from the current dtype of the query compiler so far,
    #         convert the query compiler to the given dtype if the data is lazy.
    # STEP 4: The resultant query_compiler is columnarized and set as the query_compiler for the Series.
    # STEP 5: If a name is provided, set the name.

    # STEP 1: Setting the data
    # ------------------------
    if isinstance(data, Index):
        # If the data is an Index object, convert it to a Series, and get the query_compiler.
        query_compiler = (
            data.to_series(index=None, name=name).reset_index(drop=True)._query_compiler
        )

    elif isinstance(data, Series):
        # If the data is a Series object, use its query_compiler.
        query_compiler = data._query_compiler
        if (
            copy is False
            and index is None
            and name is None
            and (dtype is None or dtype == getattr(data, "dtype", None))
        ):
            # When copy is False and no index, name, and dtype are provided, the Series is a shallow copy of the
            # original Series.
            # If a dtype is provided, and the new dtype does not match the dtype of the original query compiler,
            # self is no longer a sibling of the original DataFrame.
            self._query_compiler = query_compiler
            data._add_sibling(self)
            return

    else:
        # If the data is not a Snowpark pandas object, convert it to a query compiler.
        # The query compiler uses the '__reduced__' name internally as a column name to represent pandas
        # Series objects that are not explicitly assigned a name.
        # This helps to distinguish between an N-element Series and 1xN DataFrame.
        name = name or MODIN_UNNAMED_SERIES_LABEL
        if hasattr(data, "name") and data.name is not None:
            # If data is an object that has a name field, use that as the name of the new Series.
            name = data.name
        # If any of the values are Snowpark pandas objects, convert them to native pandas objects.
        if not isinstance(
            data, (native_pd.DataFrame, native_pd.Series, native_pd.Index)
        ) and is_list_like(data):
            if is_dict_like(data):
                data = {
                    k: v.to_list() if isinstance(v, (Index, BasePandasDataset)) else v
                    for k, v in data.items()
                }
            else:
                data = [
                    v.to_list() if isinstance(v, (Index, BasePandasDataset)) else v
                    for v in data
                ]
        query_compiler = from_pandas(
            native_pd.DataFrame(
                native_pd.Series(
                    data=data,
                    # If the index is a lazy index, handle setting it outside this block.
                    index=None if isinstance(index, (Index, Series)) else index,
                    dtype=dtype,
                    name=name,
                    copy=copy,
                    fastpath=fastpath,
                )
            )
        )._query_compiler

    # STEP 2: Setting the index
    # -------------------------
    # The index is already set if the data is a non-Snowpark pandas object.
    # If either the data or the index is a Snowpark pandas object, set the index here.
    if index is not None and (
        isinstance(index, (Index, type(self))) or isinstance(data, (Index, type(self)))
    ):
        if is_dict_like(data) or isinstance(data, (type(self), type(None))):
            # The `index` parameter is used to select the rows from `data` that will be in the resultant Series.
            # If a value in `index` is not present in `data`'s index, it will be filled with a NaN value.
            # If data is None and an index is provided, all the values in the Series will be NaN and the index
            # will be the provided index.
            query_compiler = query_compiler.reindex(
                axis=0, labels=convert_index_to_qc(index)
            )
        else:
            # Performing set index to directly set the index column (joining on row-position instead of index).
            query_compiler = query_compiler.set_index(
                convert_index_to_list_of_qcs(index)
            )

    # STEP 3: Setting the dtype if data is lazy
    # -----------------------------------------
    # If data is a Snowpark pandas object and a dtype is provided, and it does not match the current dtype of the
    # query compiler, convert the query compiler's dtype to the new dtype.
    # Local data should have the dtype parameter taken care of by the pandas constructor at the end.
    if (
        dtype is not None
        and isinstance(data, (Index, Series))
        and dtype != getattr(data, "dtype", None)
    ):
        query_compiler = query_compiler.astype(
            {col: dtype for col in query_compiler.columns}
        )

    # STEP 4 and STEP 5: Setting the query compiler and name
    # ------------------------------------------------------
    self._query_compiler = query_compiler.columnarize()
    if name is not None:
        self.name = name


def _update_inplace(self, new_query_compiler) -> None:
    """
    Update the current Series in-place using `new_query_compiler`.

    Parameters
    ----------
    new_query_compiler : BaseQueryCompiler
        QueryCompiler to use to manage the data.
    """
    super(Series, self)._update_inplace(new_query_compiler=new_query_compiler)
    # Propagate changes back to parent so that column in dataframe had the same contents
    if self._parent is not None:
        if self._parent_axis == 1 and isinstance(self._parent, DataFrame):
            self._parent[self.name] = self
        else:
            self._parent.loc[self.index] = self


# Modin uses _update_inplace to implement set_backend(inplace=True), so in modin 0.33 and newer we
# can't extend _update_inplace. To fix a query count bug specific to Snowflake in _update_inplace, we overwrite
# _update_inplace entirely instead of using the extension system.
Series._update_inplace = _update_inplace


# Since Snowpark pandas leaves all data on the warehouse, memory_usage's report of local memory
# usage isn't meaningful and is set to always return 0.
@_inherit_docstrings(native_pd.Series.memory_usage, apilink="pandas.Series")
@register_series_accessor("memory_usage")
def memory_usage(self, index: bool = True, deep: bool = False) -> int:
    """
    Return zero bytes for memory_usage
    """
    # TODO: SNOW-1264697: push implementation down to query compiler
    return 0


# Snowpark pandas has slightly different type validation from upstream modin.
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


# Snowpark pandas raises a warning before materializing data and passing to `plot`.
@register_series_accessor("plot")
@property
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
def sub(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
    """
    Return subtraction of Series and `other`, element-wise (binary operator `sub`).
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).sub(other, level=level, fill_value=fill_value, axis=axis)


register_series_accessor("subtract")(sub)


@register_series_accessor("rsub")
def rsub(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
    """
    Return subtraction of series and `other`, element-wise (binary operator `rsub`).
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).rsub(
        other, level=level, fill_value=fill_value, axis=axis
    )


@register_series_accessor("mul")
def mul(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
    """
    Return multiplication of series and `other`, element-wise (binary operator `mul`).
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).mul(other, level=level, fill_value=fill_value, axis=axis)


register_series_accessor("multiply")(mul)


@register_series_accessor("rmul")
def rmul(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
    """
    Return multiplication of series and `other`, element-wise (binary operator `mul`).
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).rmul(
        other, level=level, fill_value=fill_value, axis=axis
    )


@register_series_accessor("truediv")
def truediv(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
    """
    Return floating division of series and `other`, element-wise (binary operator `truediv`).
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).truediv(
        other, level=level, fill_value=fill_value, axis=axis
    )


register_series_accessor("div")(truediv)
register_series_accessor("divide")(truediv)


@register_series_accessor("rtruediv")
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


register_series_accessor("rdiv")(rtruediv)


@register_series_accessor("floordiv")
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
def mod(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
    """
    Return Modulo of series and `other`, element-wise (binary operator `mod`).
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).mod(other, level=level, fill_value=fill_value, axis=axis)


@register_series_accessor("rmod")
def rmod(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
    """
    Return modulo of series and `other`, element-wise (binary operator `rmod`).
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).rmod(
        other, level=level, fill_value=fill_value, axis=axis
    )


@register_series_accessor("pow")
def pow(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
    """
    Return exponential power of series and `other`, element-wise (binary operator `pow`).
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).pow(other, level=level, fill_value=fill_value, axis=axis)


@register_series_accessor("rpow")
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
def __add__(self, right):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return self.add(right)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__radd__")
def __radd__(self, left):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return self.radd(left)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__and__")
def __and__(self, other):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).__and__(other)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__rand__")
def __rand__(self, other):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).__rand__(other)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__divmod__")
def __divmod__(self, right):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return self.divmod(right)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__rdivmod__")
def __rdivmod__(self, left):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return self.rdivmod(left)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__floordiv__")
def __floordiv__(self, right):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return self.floordiv(right)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__rfloordiv__")
def __rfloordiv__(self, right):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return self.rfloordiv(right)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__mod__")
def __mod__(self, right):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return self.mod(right)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__rmod__")
def __rmod__(self, left):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return self.rmod(left)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__mul__")
def __mul__(self, right):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return self.mul(right)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__rmul__")
def __rmul__(self, left):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return self.rmul(left)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__or__")
def __or__(self, other):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).__or__(other)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__ror__")
def __ror__(self, other):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).__ror__(other)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__xor__")
def __xor__(self, other):  # pragma: no cover
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).__xor__(other)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__rxor__")
def __rxor__(self, other):  # pragma: no cover
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).__rxor__(other)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__pow__")
def __pow__(self, right):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return self.pow(right)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__rpow__")
def __rpow__(self, left):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return self.rpow(left)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__sub__")
def __sub__(self, right):
    return self.sub(right)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__rsub__")
def __rsub__(self, left):
    return self.rsub(left)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__truediv__")
def __truediv__(self, right):
    return self.truediv(right)


# Modin defaults to pandas for binary operators against native pandas/list objects.
@register_series_accessor("__rtruediv__")
def __rtruediv__(self, left):
    return self.rtruediv(left)


register_series_accessor("__iadd__")(__add__)
register_series_accessor("__imul__")(__mul__)
register_series_accessor("__ipow__")(__pow__)
register_series_accessor("__isub__")(__sub__)
register_series_accessor("__itruediv__")(__truediv__)


# Upstream Modin does validation on func that Snowpark pandas does not.
@register_series_accessor("aggregate")
def aggregate(
    self, func: AggFuncType = None, axis: Axis = 0, *args: Any, **kwargs: Any
):
    result = super(Series, self).aggregate(func, axis, *args, **kwargs)
    return result


register_series_accessor("agg")(aggregate)


# Upstream Modin does a significant amount of frontend manipulation and may default to pandas.
@register_series_accessor("apply")
def apply(
    self,
    func: AggFuncType,
    convert_dtype: bool = True,
    args: tuple[Any, ...] = (),
    **kwargs: Any,
):
    """
    Apply a function along an axis of the `BasePandasDataset`.
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    self._validate_function(func)
    new_query_compiler = self._query_compiler.apply_on_series(func, args, **kwargs)

    return self.__constructor__(query_compiler=new_query_compiler)


# Upstream modin calls to_pandas on the series.
@register_series_accessor("map")
def map(
    self,
    arg: Callable | Mapping | Series,
    na_action: Literal["ignore"] | None = None,
    **kwargs: Any,
) -> Series:
    """
    Map values of Series according to input correspondence.
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return self.__constructor__(
        query_compiler=self._query_compiler.map(arg, na_action, **kwargs)
    )


# In older versions of Snowpark pandas, overrides to base methods would automatically override
# corresponding DataFrame/Series API definitions as well. For consistency between methods, this
# is no longer the case, and DataFrame/Series must separately apply this override.


def _set_attrs(self, value: dict) -> None:  # noqa: RT01, D200
    # Use a field on the query compiler instead of self to avoid any possible ambiguity with
    # a column named "_attrs"
    self._query_compiler._attrs = copy.deepcopy(value)


def _get_attrs(self) -> dict:  # noqa: RT01, D200
    return self._query_compiler._attrs


register_series_accessor("attrs")(property(_get_attrs, _set_attrs))


# Snowpark pandas does different validation than upstream Modin.
@register_series_accessor("argmax")
def argmax(self, axis=None, skipna=True, *args, **kwargs):  # noqa: PR01, RT01, D200
    """
    Return int position of the largest value in the Series.
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    if self._query_compiler.has_multiindex():
        # The index is a MultiIndex, current logic does not support this.
        ErrorMessage.not_implemented(
            "Series.argmax is not yet supported when the index is a MultiIndex."
        )
    result = self.reset_index(drop=True).idxmax(
        axis=axis, skipna=skipna, *args, **kwargs
    )
    if not is_integer(result):  # if result is None, return -1
        result = -1
    return result


# Snowpark pandas does different validation than upstream Modin.
@register_series_accessor("argmin")
def argmin(self, axis=None, skipna=True, *args, **kwargs):  # noqa: PR01, RT01, D200
    """
    Return int position of the smallest value in the Series.
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    if self._query_compiler.has_multiindex():
        # The index is a MultiIndex, current logic does not support this.
        ErrorMessage.not_implemented(
            "Series.argmin is not yet supported when the index is a MultiIndex."
        )
    result = self.reset_index(drop=True).idxmin(
        axis=axis, skipna=skipna, *args, **kwargs
    )
    if not is_integer(result):  # if result is None, return -1
        result = -1
    return result


# Upstream Modin has a bug:
# https://github.com/modin-project/modin/issues/7334
@register_series_accessor("compare")
def compare(
    self,
    other: Series,
    align_axis: str | int = 1,
    keep_shape: bool = False,
    keep_equal: bool = False,
    result_names: tuple = ("self", "other"),
) -> Series:  # noqa: PR01, RT01, D200
    """
    Compare to another Series and show the differences.
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    if not isinstance(other, Series):
        raise TypeError(f"Cannot compare Series to {type(other)}")
    result = self.to_frame().compare(
        # TODO(https://github.com/modin-project/modin/issues/7334):
        # upstream this fix for differently named Series.
        other.rename(self.name).to_frame(),
        align_axis=align_axis,
        keep_shape=keep_shape,
        keep_equal=keep_equal,
        result_names=result_names,
    )
    if align_axis == "columns" or align_axis == 1:
        # pandas.DataFrame.Compare returns a dataframe with a multidimensional index object as the
        # columns so we have to change column object back.
        if len(result.columns) == 2:
            result.columns = native_pd.Index(result_names)
        else:
            # even if the DataFrame.compare() result has no columns, the
            # Series.compare() result always has the `result_names` as two
            # columns.
            # TODO(https://github.com/modin-project/modin/issues/5697):
            # upstream this fix to modin.

            # we have compared only one column, so DataFrame.compare()
            # should only produce 0 or 2 columns.
            assert len(result.columns) == 0
            result = pd.DataFrame([], columns=result_names, index=result.index)
    else:
        result = result.squeeze().rename(None)
    return result


# Snowpark pandas does not respect `ignore_index`, and upstream Modin does not respect `how`.
@register_series_accessor("dropna")
def dropna(
    self,
    *,
    axis: Axis = 0,
    inplace: bool = False,
    how: str | NoDefault = no_default,
):
    """
    Return a new Series with missing values removed.
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self)._dropna(axis=axis, inplace=inplace, how=how)


# Upstream Modin does not preserve the series name.
# https://github.com/modin-project/modin/issues/7375
@register_series_accessor("duplicated")
def duplicated(self, keep: DropKeep = "first"):
    """
    Indicate duplicate Series values.
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    name = self.name
    series = self.to_frame().duplicated(keep=keep)
    # we are using df.duplicated method for series but its result will lose the series name, so we preserve it here
    series.name = name
    return series


# Upstream Modin defines sum differently for series/DF, but we use the same implementation for both.
# Even though we already override sum in base_overrides, we need to do another override here because
# Modin has a separate definition in both series.py and base.py. In general, we cannot force base_overrides
# to override both methods in case the series.py version calls the superclass method.
@register_series_accessor("sum")
def sum(
    self,
    axis: Axis | None = None,
    skipna: bool = True,
    numeric_only: bool = False,
    min_count: int = 0,
    **kwargs: Any,
):
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    min_count = validate_int_kwarg(min_count, "min_count")
    kwargs.update({"min_count": min_count})
    return self._agg_helper(
        func="sum",
        axis=axis,
        skipna=skipna,
        numeric_only=numeric_only,
        **kwargs,
    )


# Snowpark pandas handles kwargs differently than modin.
@register_series_accessor("std")
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


# Snowpark pandas handles kwargs differently than modin.
@register_series_accessor("var")
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
def cat(self) -> CategoryMethods:
    return CategoryMethods(self)


# Snowpark pandas performs type validation that Modin does not.
@register_series_accessor("dt")
@property
def dt(self):  # noqa: RT01, D200
    """
    Accessor object for datetimelike properties of the Series values.
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    if not self._query_compiler.is_datetime64_any_dtype(
        idx=0, is_index=False
    ) and not self._query_compiler.is_timedelta64_dtype(idx=0, is_index=False):
        raise AttributeError("Can only use .dt accessor with datetimelike values")

    from modin.pandas.series_utils import DatetimeProperties

    return DatetimeProperties(self)


# Snowpark pandas performs type validation that Modin does not.
# Avoid naming the object "str" to avoid overwriting Python built-in "str".
@register_series_accessor("str")
@property
def _str(self):  # noqa: RT01, D200
    """
    Vectorized string functions for Series and Index.
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    if not self._query_compiler.is_string_dtype(idx=0, is_index=False):
        raise AttributeError("Can only use .str accessor with string values!")

    from modin.pandas.series_utils import StringMethods

    return StringMethods(self)


# Snowpark pandas uses an update_in_place call that upstream Modin does not.
def _set_name(self, name):
    """
    Set the value of the `name` property.

    Parameters
    ----------
    name : hashable
        Name value to set.
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    if name is None:
        name = MODIN_UNNAMED_SERIES_LABEL
    if isinstance(name, tuple):
        columns = pd.MultiIndex.from_tuples(tuples=[name])
    else:
        columns = [name]
    self._update_inplace(new_query_compiler=self._query_compiler.set_columns(columns))


register_series_accessor("name")(property(Series._get_name, _set_name))


# Modin uses len(self.index) instead of len(self), which may incur an extra query.
@register_series_accessor("empty")
@property
def empty(self) -> bool:
    return len(self) == 0


# Upstream modin uses squeeze_self instead of self_is_series.
@register_series_accessor("fillna")
def fillna(
    self,
    value: Hashable | Mapping | Series = None,
    *,
    method: FillnaOptions | None = None,
    axis: Axis | None = None,
    inplace: bool = False,
    limit: int | None = None,
    downcast: dict | None = None,
) -> Series | None:
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    if isinstance(value, BasePandasDataset) and not isinstance(value, Series):
        raise TypeError(
            '"value" parameter must be a scalar, dict or Series, but '
            + f'you passed a "{type(value).__name__}"'
        )
    return super(Series, self).fillna(
        self_is_series=True,
        value=value,
        method=method,
        axis=axis,
        inplace=inplace,
        limit=limit,
        downcast=downcast,
    )


# Snowpark pandas defines a custom GroupBy object
@register_series_accessor("groupby")
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
    from modin.pandas.groupby import SeriesGroupBy
    from snowflake.snowpark.modin.plugin.extensions.dataframe_groupby_overrides import (
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
        drop=False,
        dropna=dropna,
        backend_pinned=self.is_backend_pinned(),
    )


# Snowpark pandas should avoid defaulting to pandas (the current Modin upstream version needs
# Index._is_memory_usage_qualified to be implemented).
@register_series_accessor("info")
def info(
    self,
    verbose: bool | None = None,
    buf: IO[str] | None = None,
    max_cols: int | None = None,
    memory_usage: bool | str | None = None,
    show_counts: bool = True,
):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return self._default_to_pandas(
        native_pd.Series.info,
        verbose=verbose,
        buf=buf,
        max_cols=max_cols,
        memory_usage=memory_usage,
        show_counts=show_counts,
    )


# Modin defaults to pandas for _qcut.
@register_series_accessor("_qcut")
def _qcut(
    self,
    q: int | ListLike,
    retbins: bool = False,
    duplicates: Literal["raise", "drop"] = "raise",
) -> Series:
    """
    Quantile-based discretization function.

    See SnowflakeQueryCompiler.qcut for details.
    """
    return self.__constructor__(
        query_compiler=self._query_compiler.qcut(q, retbins, duplicates)
    )


# Snowpark pandas ignores `inplace` (possibly an error?) and performs additional validation.
@register_series_accessor("replace")
def replace(
    self,
    to_replace=None,
    value=no_default,
    inplace=False,
    limit=None,
    regex=False,
    method: str | NoDefault = no_default,
):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    inplace = validate_bool_kwarg(inplace, "inplace")
    # The following errors cannot be raised by query compiler because we don't know
    # if frontend object is Series or DataFrame.
    if to_replace is not None and is_dict_like(value):
        raise ValueError(
            "In Series.replace 'to_replace' must be None if the 'value' is dict-like"
        )
    if is_dict_like(to_replace) and value != no_default:
        raise ValueError(
            "In Series.replace 'to_replace' cannot be dict-like if 'value' is provided"
        )
    new_query_compiler = self._query_compiler.replace(
        to_replace=to_replace,
        value=value,
        limit=limit,
        regex=regex,
        method=method,
    )
    return self._create_or_update_from_compiler(new_query_compiler, inplace)


# Upstream Modin reset_index produces an extra query and performs a relative import of DataFrame.
@register_series_accessor("reset_index")
def reset_index(
    self,
    level=None,
    drop=False,
    name=no_default,
    inplace=False,
    allow_duplicates=False,
):
    """
    Generate a new Series with the index reset.
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    if drop:
        name = self.name
    elif name is no_default:
        # For backwards compatibility, keep columns as [0] instead of
        #  [None] when self.name is None
        name = 0 if self.name is None else self.name

    if not drop and inplace:
        raise TypeError("Cannot reset_index inplace on a Series to create a DataFrame")
    else:
        obj = self.copy()
        obj.name = name
        new_query_compiler = obj._query_compiler.reset_index(
            drop=drop,
            level=level,
            col_level=0,
            col_fill="",
            allow_duplicates=allow_duplicates,
            names=None,
        )
        return self._create_or_update_from_compiler(new_query_compiler, inplace)


# Snowpark pandas performs additional type validation.
@register_series_accessor("set_axis")
def set_axis(
    self,
    labels: IndexLabel,
    *,
    axis: Axis = 0,
    copy: bool | NoDefault = no_default,  # ignored
):
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    if not is_scalar(axis):
        raise TypeError(f"{type(axis).__name__} is not a valid type for axis.")
    return super(Series, self).set_axis(
        labels=labels,
        # 'rows', 'index, and 0 are valid axis values for Series.
        # 'columns' and 1 are valid axis values only for DataFrame.
        axis=native_pd.Series._get_axis_name(axis),
        copy=copy,
    )


# Snowpark pandas does different validation.
@register_series_accessor("rename")
def rename(
    self,
    index: Renamer | Hashable | None = None,
    *,
    axis: Axis | None = None,
    copy: bool | None = None,
    inplace: bool = False,
    level: Level | None = None,
    errors: IgnoreRaise = "ignore",
) -> Series | None:
    """
    Alter Series index labels or name.
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    if axis is not None:
        # make sure we raise if an invalid 'axis' is passed.
        # note: axis is unused. It's needed for compatibility with DataFrame.
        self._get_axis_number(axis)

    if copy is not None:
        WarningMessage.ignored_argument(
            operation="series.rename",
            argument="copy",
            message="copy parameter has been ignored with Snowflake execution engine",
        )

    if callable(index) or is_dict_like(index):
        if isinstance(index, dict):
            index = Series(index)
        new_qc = self._query_compiler.rename(
            index_renamer=index, level=level, errors=errors
        )
        new_series = self._create_or_update_from_compiler(
            new_query_compiler=new_qc, inplace=inplace
        )
        if not inplace and hasattr(self, "name"):
            new_series.name = self.name
        return new_series
    else:
        # just change Series.name
        if inplace:
            self.name = index
        else:
            self_cp = self.copy()
            self_cp.name = index
            return self_cp


# In some cases, modin after 0.30.1 returns a DatetimeArray instead of a numpy array. This
# still differs from the expected pandas behavior, which would return DatetimeIndex
# (see SNOW-1019312).
@register_series_accessor("unique")
def unique(self) -> ArrayLike:  # noqa: RT01, D200
    """
    Return unique values of Series object.
    """
    # `values` can't be used here because it performs unnecessary conversion,
    # after which the result type does not match the pandas
    return self.__constructor__(query_compiler=self._query_compiler.unique()).to_numpy()


# Modin defaults to pandas for some arguments for unstack
@register_series_accessor("unstack")
def unstack(
    self,
    level: int | str | list = -1,
    fill_value: int | str | dict = None,
    sort: bool = True,
):
    """
    Unstack, also known as pivot, Series with MultiIndex to produce DataFrame.
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    from modin.pandas.dataframe import DataFrame

    # We can't unstack a Series object, if we don't have a MultiIndex.
    if self._query_compiler.has_multiindex:
        result = DataFrame(
            query_compiler=self._query_compiler.unstack(
                level, fill_value, sort, is_series_input=True
            )
        )
    else:
        raise ValueError(  # pragma: no cover
            f"index must be a MultiIndex to unstack, {type(self.index)} was passed"
        )

    return result


# Snowpark pandas does an extra check on `len(ascending)`.
@register_series_accessor("sort_values")
def sort_values(
    self,
    axis: Axis = 0,
    ascending: bool | int | Sequence[bool] | Sequence[int] = True,
    inplace: bool = False,
    kind: str = "quicksort",
    na_position: str = "last",
    ignore_index: bool = False,
    key: IndexKeyFunc | None = None,
):
    """
    Sort by the values.
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions

    if is_list_like(ascending) and len(ascending) != 1:
        raise ValueError(f"Length of ascending ({len(ascending)}) must be 1 for Series")

    if axis is not None:
        # Validate `axis`
        self._get_axis_number(axis)
    # Validate inplace, ascending and na_position.
    inplace = validate_bool_kwarg(inplace, "inplace")
    ascending = validate_ascending(ascending)
    if na_position not in get_args(NaPosition):
        # Same error message as native pandas for invalid 'na_position' value.
        raise ValueError(f"invalid na_position: {na_position}")

    # Convert 'ascending' to sequence if needed.
    if not isinstance(ascending, Sequence):
        ascending = [ascending]
    result = self._query_compiler.sort_rows_by_column_values(
        self._query_compiler.columns,
        ascending,
        kind,
        na_position,
        ignore_index,
        key,
        include_index=False,
    )
    return self._create_or_update_from_compiler(result, inplace=inplace)


# Upstream Modin defaults at the frontend layer.
@register_series_accessor("where")
def where(
    self,
    cond: DataFrame | Series | Callable | AnyArrayLike,
    other: DataFrame | Series | Callable | Scalar | None = np.nan,
    inplace: bool = False,
    axis: Axis | None = None,
    level: Level | None = None,
):
    """
    Replace values where the condition is False.
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return super(Series, self).where(
        cond,
        other=other,
        inplace=inplace,
        axis=axis,
        level=level,
    )


# Upstream modin defaults to pandas for some arguments.
@register_series_accessor("value_counts")
def value_counts(
    self,
    normalize: bool = False,
    sort: bool = True,
    ascending: bool = False,
    bins: int | None = None,
    dropna: bool = True,
):
    """
    Return a Series containing counts of unique values.
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return self.__constructor__(
        query_compiler=self._query_compiler.value_counts(
            subset=None,
            normalize=normalize,
            sort=sort,
            ascending=ascending,
            bins=bins,
            dropna=dropna,
        ).set_index_names([self.name]),
        name="proportion" if normalize else "count",
    )


# The `suffix` parameter is documented but according to pandas maintainers, "not public."
# https://github.com/pandas-dev/pandas/issues/54806
# The parameter is ignored in upstream modin, but Snowpark pandas wants to error if the parameter is given.
@register_series_accessor("shift")
def shift(
    self,
    periods: int | Sequence[int] = 1,
    freq=None,
    axis: Axis = 0,
    fill_value: Hashable = no_default,
    suffix: str | None = None,
):
    """
    Shift index by desired number of periods with an optional time `freq`.
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    if axis == 1:
        # pandas compatible error.
        raise ValueError("No axis named 1 for object type Series")

    return super(Series, self).shift(periods, freq, axis, fill_value, suffix)


# Snowpark pandas uses len(self) instead of len(index), saving a query in some cases.
@register_series_accessor("squeeze")
def squeeze(self, axis: Axis | None = None):
    """
    Squeeze 1 dimensional axis objects into scalars.
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    if axis is not None:
        # Validate `axis`
        native_pd.Series._get_axis_number(axis)
    if len(self) == 1:
        return self._reduce_dimension(self._query_compiler)
    else:
        return self.copy()


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


# Modin uses the query compiler to_list method, which we should try to implement instead of calling self.values.
@register_series_accessor("to_list")
@materialization_warning
def to_list(self) -> list:
    """
    Return a list of the values.
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return self.values.tolist()


register_series_accessor("tolist")(to_list)


@materialization_warning
@register_series_accessor("to_dict")
def to_dict(self, into: type[dict] = dict) -> dict:
    """
    Convert Series to {label -> value} dict or dict-like object.
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return self._to_pandas().to_dict(into=into)


@materialization_warning
@register_series_accessor("to_numpy")
def to_numpy(
    self,
    dtype: npt.DTypeLike | None = None,
    copy: bool = False,
    na_value: object = no_default,
    **kwargs: Any,
) -> np.ndarray:
    """
    Return the NumPy ndarray representing the values in this Series or Index.
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    return (
        super(Series, self)
        .to_numpy(
            dtype=dtype,
            copy=copy,
            na_value=na_value,
            **kwargs,
        )
        .flatten()
    )


# Snowpark pandas has the extra `statement_params` argument.
@register_series_accessor("_to_pandas")
@materialization_warning
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
