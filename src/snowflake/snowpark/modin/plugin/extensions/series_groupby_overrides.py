#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

# Licensed to Modin Development Team under one or more contributor license agreements.
# See the NOTICE file distributed with this work for additional information regarding
# copyright ownership.  The Modin Development Team licenses this file to you under the
# Apache License, Version 2.0 (the "License"); you may not use this file except in
# compliance with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under
# the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific language
# governing permissions and limitations under the License.

# Code in this file may constitute partial or total reimplementation, or modification of
# existing code originally distributed by the Modin project, under the Apache License,
# Version 2.0.

"""Implement GroupBy public API as pandas does."""

import functools
from typing import Any, Literal, Optional

import modin.pandas as pd
from modin.pandas.groupby import SeriesGroupBy
import numpy as np  # noqa: F401
import pandas
import pandas.core.groupby
from modin.pandas import Series
from pandas._typing import (
    AggFuncType,
)
from pandas.core.dtypes.common import is_dict_like
from pandas.errors import SpecificationError

from snowflake.snowpark.modin.plugin._internal.utils import (
    INDEX_LABEL,
)

from snowflake.snowpark.modin.plugin.utils.error_message import ErrorMessage
from snowflake.snowpark.modin.utils import (
    MODIN_UNNAMED_SERIES_LABEL,
)

from modin.pandas.api.extensions import (
    register_series_groupby_accessor,
)

register_ser_groupby_override = functools.partial(
    register_series_groupby_accessor, backend="Snowflake"
)


@register_ser_groupby_override("_iter")
@property
def _iter(self):
    """
    Construct a tuple of (group_id, Series) tuples to allow iteration over groups.

    Returns
    -------
    generator
        Generator expression of GroupBy object broken down into tuples for iteration.
    """
    # TODO: SNOW-1063350: Modin upgrade - modin.pandas.groupby.SeriesGroupBy functions
    indices = self.indices
    group_ids = indices.keys()

    assert self._axis == 0, (
        "GroupBy does not yet support axis=1. "
        "A NotImplementedError should have already been raised."
    )

    return (
        (
            k,
            pd.Series(
                query_compiler=self._query_compiler.getitem_row_array(indices[k])
            ),
        )
        for k in (sorted(group_ids) if self._sort else group_ids)
    )


###########################################################################
# Indexing, iteration
###########################################################################


@register_ser_groupby_override("get_group")
def get_group(self, name, obj=None):
    ErrorMessage.method_not_implemented_error(name="get_group", class_="SeriesGroupBy")


###########################################################################
# Function application
###########################################################################


@register_ser_groupby_override("apply")
def apply(self, func, *args, include_groups=True, **kwargs):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.SeriesGroupBy functions
    if not callable(func):
        raise NotImplementedError("No support for non-callable `func`")
    dataframe_result = pd.DataFrame(
        query_compiler=self._query_compiler.groupby_apply(
            self._by,
            agg_func=func,
            axis=self._axis,
            groupby_kwargs=self._kwargs,
            agg_args=args,
            agg_kwargs=kwargs,
            include_groups=include_groups,
            # TODO(https://github.com/modin-project/modin/issues/7096):
            # upstream the series_groupby param to Modin
            series_groupby=True,
        )
    )
    if dataframe_result.columns.equals(pandas.Index([MODIN_UNNAMED_SERIES_LABEL])):
        # rename to the last column of self._df
        # note that upstream modin does not do this yet due to
        # https://github.com/modin-project/modin/issues/7097
        return dataframe_result.squeeze(axis=1).rename(self._df.columns[-1])
    return dataframe_result


@register_ser_groupby_override("aggregate")
def aggregate(
    self,
    func: Optional[AggFuncType] = None,
    *args: Any,
    engine: Optional[Literal["cython", "numba"]] = None,
    engine_kwargs: Optional[dict[str, bool]] = None,
    **kwargs: Any,
):
    # TODO: SNOW-1063350: Modin upgrade - modin.pandas.groupby.SeriesGroupBy functions
    if is_dict_like(func):
        raise SpecificationError("nested renamer is not supported")

    return super(SeriesGroupBy, self).aggregate(
        func, *args, engine=engine, engine_kwargs=engine_kwargs, **kwargs
    )


register_ser_groupby_override("agg")(aggregate)


###########################################################################
# Computations / descriptive stats
###########################################################################


@register_ser_groupby_override("cov")
def cov(self, min_periods=None, ddof=1):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    ErrorMessage.method_not_implemented_error(name="cov", class_="GroupBy")


@register_ser_groupby_override("corr")
def corr(self, method="pearson", min_periods=1):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    ErrorMessage.method_not_implemented_error(name="corr", class_="GroupBy")


@register_ser_groupby_override("describe")
def describe(self, percentiles=None, include=None, exclude=None):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    ErrorMessage.method_not_implemented_error(name="describe", class_="GroupBy")


@register_ser_groupby_override("hist")
def hist(
    self,
    by=None,
    grid=True,
    xlabelsize=None,
    xrot=None,
    ylabelsize=None,
    yrot=None,
    ax=None,
    sharex=False,
    sharey=False,
    figsize=None,
    layout=None,
    bins=10,
    backend=None,
    legend=False,
    **kwargs,
):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    ErrorMessage.method_not_implemented_error(name="hist", class_="GroupBy")


@register_ser_groupby_override("is_monotonic_decreasing")
@property
def is_monotonic_decreasing(self):
    # TODO: SNOW-1063350: Modin upgrade - modin.pandas.groupby.SeriesGroupBy functions
    ErrorMessage.method_not_implemented_error(
        name="is_monotonic_decreasing", class_="GroupBy"
    )


@register_ser_groupby_override("is_monotonic_increasing")
@property
def is_monotonic_increasing(self):
    # TODO: SNOW-1063350: Modin upgrade - modin.pandas.groupby.SeriesGroupBy functions
    ErrorMessage.method_not_implemented_error(
        name="is_monotonic_increasing", class_="GroupBy"
    )


@register_ser_groupby_override("nlargest")
def nlargest(self, n=5, keep="first"):
    # TODO: SNOW-1063350: Modin upgrade - modin.pandas.groupby.SeriesGroupBy functions
    ErrorMessage.method_not_implemented_error(name="nlargest", class_="GroupBy")


@register_ser_groupby_override("nsmallest")
def nsmallest(self, n=5, keep="first"):
    # TODO: SNOW-1063350: Modin upgrade - modin.pandas.groupby.SeriesGroupBy functions
    ErrorMessage.method_not_implemented_error(name="nsmallest", class_="GroupBy")


@register_ser_groupby_override("unique")
def unique(self):
    return self._wrap_aggregation(
        type(self._query_compiler).groupby_unique,
        numeric_only=False,
    )


@register_ser_groupby_override("size")
def size(self):
    # TODO: Remove this once SNOW-1478924 is fixed
    result = super(SeriesGroupBy, self).size()
    if isinstance(result, Series):
        return result.rename(self._df.columns[-1])
    else:
        return result


@register_ser_groupby_override("value_counts")
def value_counts(
    self,
    normalize: bool = False,
    sort: bool = True,
    ascending: bool = False,
    bins: Optional[int] = None,
    dropna: bool = True,
):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.SeriesGroupBy functions
    # Modin upstream defaults to pandas for this method, so we need to either override this or
    # rewrite this logic to be friendlier to other backends.
    #
    # Unlike DataFrameGroupBy, SeriesGroupBy has an additional `bins` parameter.
    qc = self._query_compiler
    # The "by" list becomes the new index, which we then perform the group by on. We call
    # reset_index to let the query compiler treat it as a data column so it can be grouped on.
    if self._by is not None:
        qc = (
            qc.set_index_from_series(pd.Series(self._by)._query_compiler)
            .set_index_names([INDEX_LABEL])
            .reset_index()
        )
    result_qc = qc.groupby_value_counts(
        by=[INDEX_LABEL],
        axis=self._axis,
        groupby_kwargs=self._kwargs,
        subset=None,
        normalize=normalize,
        sort=sort,
        ascending=ascending,
        bins=bins,
        dropna=dropna,
    )
    # Reset the names in the MultiIndex
    result_qc = result_qc.set_index_names([None] * result_qc.nlevels())
    return pd.Series(
        query_compiler=result_qc,
        name="proportion" if normalize else "count",
    )
