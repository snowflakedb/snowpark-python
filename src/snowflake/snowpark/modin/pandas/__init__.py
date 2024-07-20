#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
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

import sys
import warnings
from typing import Any

import pandas

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    from pandas import describe_option  # noqa: F401
    from pandas import get_option  # noqa: F401
    from pandas import option_context  # noqa: F401
    from pandas import reset_option  # noqa: F401
    from pandas import (  # noqa: F401
        NA,
        ArrowDtype,
        BooleanDtype,
        Categorical,
        CategoricalDtype,
        CategoricalIndex,
        DateOffset,
        DatetimeIndex,
        DatetimeTZDtype,
        ExcelWriter,
        Flags,
        Float32Dtype,
        Float64Dtype,
        Grouper,
        IndexSlice,
        Int8Dtype,
        Int16Dtype,
        Int32Dtype,
        Int64Dtype,
        Interval,
        IntervalDtype,
        IntervalIndex,
        MultiIndex,
        NamedAgg,
        NaT,
        Period,
        PeriodDtype,
        PeriodIndex,
        RangeIndex,
        SparseDtype,
        StringDtype,
        Timedelta,
        TimedeltaIndex,
        Timestamp,
        UInt8Dtype,
        UInt16Dtype,
        UInt32Dtype,
        UInt64Dtype,
        api,
        array,
        bdate_range,
        eval,
        factorize,
        from_dummies,
        infer_freq,
        interval_range,
        offsets,
        options,
        period_range,
        set_eng_float_format,
        set_option,
        test,
        timedelta_range,
    )

# TODO: SNOW-851745 make sure add all Snowpark pandas API general functions
from modin.pandas import plotting  # type: ignore[import]

from snowflake.snowpark.modin.pandas.dataframe import DataFrame
from snowflake.snowpark.modin.pandas.general import (
    concat,
    crosstab,
    cut,
    date_range,
    get_dummies,
    isna,
    isnull,
    lreshape,
    melt,
    merge,
    merge_asof,
    merge_ordered,
    notna,
    notnull,
    pivot,
    pivot_table,
    qcut,
    to_datetime,
    to_numeric,
    to_timedelta,
    unique,
    value_counts,
    wide_to_long,
)
from snowflake.snowpark.modin.pandas.io import (
    ExcelFile,
    HDFStore,
    json_normalize,
    read_clipboard,
    read_csv,
    read_excel,
    read_feather,
    read_fwf,
    read_gbq,
    read_hdf,
    read_html,
    read_json,
    read_orc,
    read_parquet,
    read_pickle,
    read_sas,
    read_spss,
    read_sql,
    read_sql_query,
    read_sql_table,
    read_stata,
    read_table,
    read_xml,
    to_pickle,
)
from snowflake.snowpark.modin.pandas.series import Series
from snowflake.snowpark.modin.plugin._internal.session import SnowpandasSessionHolder

# The extensions assigned to this module
_PD_EXTENSIONS_: dict = {}

# base needs to be re-exported in order to properly override docstrings for BasePandasDataset
# moving this import higher prevents sphinx from building documentation (??)
from snowflake.snowpark.modin.pandas import base  # isort: skip  # noqa: E402,F401

import snowflake.snowpark.modin.plugin.extensions.pd_extensions as pd_extensions  # isort: skip  # noqa: E402,F401
import snowflake.snowpark.modin.plugin.extensions.pd_overrides  # isort: skip  # noqa: E402,F401
from snowflake.snowpark.modin.plugin.extensions.pd_overrides import (  # isort: skip  # noqa: E402,F401
    Index,
)
import snowflake.snowpark.modin.plugin.extensions.dataframe_extensions  # isort: skip  # noqa: E402,F401
import snowflake.snowpark.modin.plugin.extensions.dataframe_overrides  # isort: skip  # noqa: E402,F401
import snowflake.snowpark.modin.plugin.extensions.series_extensions  # isort: skip  # noqa: E402,F401
import snowflake.snowpark.modin.plugin.extensions.series_overrides  # isort: skip  # noqa: E402,F401


def __getattr__(name: str) -> Any:
    """
    Overrides getattr on the module to enable extensions.
    Parameters
    ----------
    name : str
        The name of the attribute being retrieved.
    Returns
    -------
    Attribute
        Returns the extension attribute, if it exists, otherwise returns the attribute
        imported in this file.
    """
    try:
        return _PD_EXTENSIONS_.get(name, globals()[name])
    except KeyError:
        raise AttributeError(
            f"module 'snowflake.snowpark.modin.pandas' has no attribute '{name}'"
        )


__all__ = [  # noqa: F405
    "DataFrame",
    "Series",
    "read_csv",
    "read_parquet",
    "read_json",
    "read_html",
    "read_clipboard",
    "read_excel",
    "read_hdf",
    "read_feather",
    "read_stata",
    "read_sas",
    "read_pickle",
    "read_sql",
    "read_gbq",
    "read_table",
    "read_spss",
    "read_orc",
    "json_normalize",
    "concat",
    "eval",
    "cut",
    "factorize",
    "test",
    "qcut",
    "to_datetime",
    "get_dummies",
    "isna",
    "isnull",
    "merge",
    "pivot_table",
    "date_range",
    "Index",
    "MultiIndex",
    "Series",
    "bdate_range",
    "period_range",
    "DatetimeIndex",
    "to_timedelta",
    "set_eng_float_format",
    "options",
    "set_option",
    "CategoricalIndex",
    "Timedelta",
    "Timestamp",
    "NaT",
    "PeriodIndex",
    "Categorical",
    "__version__",
    "melt",
    "crosstab",
    "plotting",
    "Interval",
    "UInt8Dtype",
    "UInt16Dtype",
    "UInt32Dtype",
    "UInt64Dtype",
    "SparseDtype",
    "Int8Dtype",
    "Int16Dtype",
    "Int32Dtype",
    "Int64Dtype",
    "CategoricalDtype",
    "DatetimeTZDtype",
    "IntervalDtype",
    "PeriodDtype",
    "BooleanDtype",
    "StringDtype",
    "NA",
    "RangeIndex",
    "TimedeltaIndex",
    "IntervalIndex",
    "IndexSlice",
    "Grouper",
    "array",
    "Period",
    "show_versions",
    "DateOffset",
    "timedelta_range",
    "infer_freq",
    "interval_range",
    "ExcelWriter",
    "read_fwf",
    "read_sql_table",
    "read_sql_query",
    "ExcelFile",
    "to_pickle",
    "HDFStore",
    "lreshape",
    "wide_to_long",
    "merge_asof",
    "merge_ordered",
    "notnull",
    "notna",
    "pivot",
    "to_numeric",
    "unique",
    "value_counts",
    "NamedAgg",
    "api",
    "read_xml",
    "ArrowDtype",
    "Flags",
    "Float32Dtype",
    "Float64Dtype",
    "from_dummies",
]

del pandas

# Make SnowpandasSessionHolder this module's and modin.pandas's __class__ so that we can make
# "session" a lazy property of the modules.
# This implementation follows Python's suggestion here:
# https://docs.python.org/3.12/reference/datamodel.html#customizing-module-attribute-access
sys.modules[__name__].__class__ = SnowpandasSessionHolder
# When docs are generated, modin.pandas is not imported, so do not perform this overwrite
if "modin.pandas" in sys.modules:
    sys.modules["modin.pandas"].__class__ = SnowpandasSessionHolder

_SKIP_TOP_LEVEL_ATTRS = [
    # __version__ and show_versions are exported by __all__, but not currently defined in Snowpark pandas.
    "__version__",
    "show_versions",
    # SNOW-1316523: Snowpark pandas should re-export the native pandas.api submodule, but doing so
    # would override register_pd_accessor and similar methods defined in our own modin.pandas.extensions
    # module.
    "api",
]

# Manually re-export the members of the pd_extensions namespace, which are not declared in __all__.
_EXTENSION_ATTRS = ["read_snowflake", "to_snowflake", "to_snowpark", "to_pandas"]
# We also need to re-export native_pd.offsets, since modin.pandas doesn't re-export it.
# snowflake.snowpark.pandas.base also needs to be re-exported to make docstring overrides for BasePandasDataset work.
_ADDITIONAL_ATTRS = ["offsets", "base"]

# This code should eventually be moved into the `snowflake.snowpark.modin.plugin` module instead.
# Currently, trying to do so would result in incorrect results because `snowflake.snowpark.modin.pandas`
# import submodules of `snowflake.snowpark.modin.plugin`, so we would encounter errors due to
# partially initialized modules.
import modin.pandas.api.extensions as _ext  # type: ignore  # noqa: E402

# This loop overrides all methods in the `modin.pandas` namespace so users can obtain Snowpark pandas objects from it.
for name in __all__ + _ADDITIONAL_ATTRS:
    if name not in _SKIP_TOP_LEVEL_ATTRS:
        # instead of using this as a decorator, we can call the function directly
        _ext.register_pd_accessor(name)(__getattr__(name))

for name in _EXTENSION_ATTRS:
    _ext.register_pd_accessor(name)(getattr(pd_extensions, name))


# TODO: https://github.com/modin-project/modin/issues/7233
# Upstream Modin does not properly render property names in default2pandas warnings, so we need
# to override DefaultMethod.register.
import modin.core.dataframe.algebra.default2pandas  # type: ignore  # noqa: E402

import snowflake.snowpark.modin.core.dataframe.algebra.default2pandas.default  # noqa: E402

modin.core.dataframe.algebra.default2pandas.default.DefaultMethod.register = (
    snowflake.snowpark.modin.core.dataframe.algebra.default2pandas.default.DefaultMethod.register
)
