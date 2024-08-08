#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

"""
The functions in this file are not implemented in Snowpark pandas. In the future, they
should raise NotImplementedError at the query compiler layer, but doing so requires a longer-term
effort.

We currently test unsupported APIs under tests/unit/modin/test_unsupported.py, which does not initialize
a session. As such, many frontend methods have additional query compiler API calls that would have to
be mocked before the NotImplementedError can appropriately be raised.
"""
from __future__ import annotations

import pickle as pkl
from typing import Any

import numpy as np
import pandas
from modin.pandas.base import BasePandasDataset
from pandas._libs import lib
from pandas._libs.lib import no_default
from pandas._typing import (
    Axis,
    CompressionOptions,
    StorageOptions,
    TimedeltaConvertibleTypes,
)

from snowflake.snowpark.modin.pandas.api.extensions import (
    register_dataframe_accessor,
    register_series_accessor,
)
from snowflake.snowpark.modin.plugin._internal.telemetry import (
    snowpark_pandas_telemetry_method_decorator,
)
from snowflake.snowpark.modin.plugin.utils.error_message import base_not_implemented


def register_base_not_implemented():
    def decorator(base_method: Any):
        func = snowpark_pandas_telemetry_method_decorator(
            base_not_implemented()(base_method)
        )
        register_series_accessor(base_method.__name__)(func)
        register_dataframe_accessor(base_method.__name__)(func)
        return func

    return decorator


@register_base_not_implemented()
def align(
    self,
    other,
    join="outer",
    axis=None,
    level=None,
    copy=None,
    fill_value=None,
    method=lib.no_default,
    limit=lib.no_default,
    fill_axis=lib.no_default,
    broadcast_axis=lib.no_default,
):  # noqa: PR01, RT01, D200
    pass


@register_base_not_implemented()
def asof(self, where, subset=None):  # noqa: PR01, RT01, D200
    pass


@register_base_not_implemented()
def at_time(self, time, asof=False, axis=None):  # noqa: PR01, RT01, D200
    pass


@register_base_not_implemented()
def between_time(
    self: BasePandasDataset,
    start_time,
    end_time,
    inclusive: str | None = None,
    axis=None,
):  # noqa: PR01, RT01, D200
    pass


@register_base_not_implemented()
def bool(self):  # noqa: RT01, D200
    pass


@register_base_not_implemented()
def clip(
    self, lower=None, upper=None, axis=None, inplace=False, *args, **kwargs
):  # noqa: PR01, RT01, D200
    pass


@register_base_not_implemented()
def combine(self, other, func, fill_value=None, **kwargs):  # noqa: PR01, RT01, D200
    pass


@register_base_not_implemented()
def combine_first(self, other):  # noqa: PR01, RT01, D200
    pass


@register_base_not_implemented()
def droplevel(self, level, axis=0):  # noqa: PR01, RT01, D200
    pass


@register_base_not_implemented()
def explode(self, column, ignore_index: bool = False):  # noqa: PR01, RT01, D200
    pass


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
    pass


@register_base_not_implemented()
def filter(
    self, items=None, like=None, regex=None, axis=None
):  # noqa: PR01, RT01, D200
    pass


@register_base_not_implemented()
def infer_objects(
    self, copy: bool | None = None
) -> BasePandasDataset:  # pragma: no cover # noqa: RT01, D200
    pass


@register_base_not_implemented()
def kurt(self, axis=no_default, skipna=True, numeric_only=False, **kwargs):
    pass


@register_base_not_implemented()
def kurtosis(self, axis=no_default, skipna=True, numeric_only=False, **kwargs):
    pass


@register_base_not_implemented()
def mode(self, axis=0, numeric_only=False, dropna=True):  # noqa: PR01, RT01, D200
    pass


@register_base_not_implemented()
def pipe(self, func, *args, **kwargs):  # noqa: PR01, RT01, D200
    pass


@register_base_not_implemented()
def pop(self, item):  # noqa: PR01, RT01, D200
    pass


@register_base_not_implemented()
def reindex_like(
    self, other, method=None, copy=True, limit=None, tolerance=None
):  # noqa: PR01, RT01, D200
    pass


@register_base_not_implemented()
def reorder_levels(self, order, axis=0):  # noqa: PR01, RT01, D200
    pass


@register_base_not_implemented()
def sem(
    self,
    axis: Axis | None = None,
    skipna: bool = True,
    ddof: int = 1,
    numeric_only=False,
    **kwargs,
):  # noqa: PR01, RT01, D200
    pass


@register_base_not_implemented()
def set_flags(
    self, *, copy: bool = False, allows_duplicate_labels: bool | None = None
):  # noqa: PR01, RT01, D200
    pass


@register_base_not_implemented()
def swapaxes(self, axis1, axis2, copy=True):  # noqa: PR01, RT01, D200
    pass


@register_base_not_implemented()
def swaplevel(self, i=-2, j=-1, axis=0):  # noqa: PR01, RT01, D200
    pass


@register_base_not_implemented()
def to_clipboard(
    self, excel=True, sep=None, **kwargs
):  # pragma: no cover  # noqa: PR01, RT01, D200
    pass


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
    pass


@register_base_not_implemented()
def to_hdf(
    self, path_or_buf, key, format="table", **kwargs
):  # pragma: no cover  # noqa: PR01, RT01, D200
    pass


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
    pass


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
    pass


@register_base_not_implemented()
def to_markdown(
    self,
    buf=None,
    mode: str = "wt",
    index: bool = True,
    storage_options: StorageOptions = None,
    **kwargs,
):  # noqa: PR01, RT01, D200
    pass


@register_base_not_implemented()
def to_pickle(
    self,
    path,
    compression: CompressionOptions = "infer",
    protocol: int = pkl.HIGHEST_PROTOCOL,
    storage_options: StorageOptions = None,
):  # pragma: no cover  # noqa: PR01, D200
    pass


@register_base_not_implemented()
def to_string(
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
    justify=None,
    max_rows=None,
    min_rows=None,
    max_cols=None,
    show_dimensions=False,
    decimal=".",
    line_width=None,
    max_colwidth=None,
    encoding=None,
):  # noqa: PR01, RT01, D200
    pass


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
    pass


@register_base_not_implemented()
def to_timestamp(
    self, freq=None, how="start", axis=0, copy=True
):  # noqa: PR01, RT01, D200
    pass


@register_base_not_implemented()
def to_xarray(self):  # noqa: PR01, RT01, D200
    pass


@register_base_not_implemented()
def truncate(
    self, before=None, after=None, axis=None, copy=True
):  # noqa: PR01, RT01, D200
    pass


@register_base_not_implemented()
def tz_convert(self, tz, axis=0, level=None, copy=True):  # noqa: PR01, RT01, D200
    pass


@register_base_not_implemented()
def tz_localize(
    self, tz, axis=0, level=None, copy=True, ambiguous="raise", nonexistent="raise"
):  # noqa: PR01, RT01, D200
    pass


@register_base_not_implemented()
def __array_wrap__(self, result, context=None):
    pass


@register_base_not_implemented()
def __finalize__(self, other, method=None, **kwargs):
    pass


@register_base_not_implemented()
def __sizeof__(self):
    pass
