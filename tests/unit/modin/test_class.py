#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin.extensions.index import Index
from snowflake.snowpark.modin.plugin._internal.utils import MODIN_IS_AT_LEAST_0_33_0


def test_class_equivalence():
    # all classes imported from native pandas modin/pandas/__init__.py should be listed and tested here
    # TODO: SNOW-837070 make these modules as a list in src/snowflake/snowpark/plugin/__init__.py
    # so we can test from this list
    assert pd.describe_option is native_pd.describe_option
    assert pd.get_option is native_pd.get_option
    assert pd.option_context is native_pd.option_context
    assert pd.reset_option is native_pd.reset_option
    assert pd.NA is native_pd.NA
    assert pd.ArrowDtype is native_pd.ArrowDtype
    assert pd.BooleanDtype is native_pd.BooleanDtype
    assert pd.Categorical is native_pd.Categorical
    assert pd.CategoricalDtype is native_pd.CategoricalDtype
    assert pd.CategoricalIndex is native_pd.CategoricalIndex
    assert pd.DateOffset is native_pd.DateOffset
    assert pd.DatetimeTZDtype is native_pd.DatetimeTZDtype
    assert pd.ExcelWriter is native_pd.ExcelWriter
    assert pd.Flags is native_pd.Flags
    assert pd.Float32Dtype is native_pd.Float32Dtype
    assert pd.Float64Dtype is native_pd.Float64Dtype
    assert pd.Grouper is native_pd.Grouper
    assert pd.Index is Index
    assert pd.IndexSlice is native_pd.IndexSlice
    assert pd.Int8Dtype is native_pd.Int8Dtype
    assert pd.Int16Dtype is native_pd.Int16Dtype
    assert pd.Int32Dtype is native_pd.Int32Dtype
    assert pd.Int64Dtype is native_pd.Int64Dtype
    assert pd.Interval is native_pd.Interval
    assert pd.IntervalDtype is native_pd.IntervalDtype
    assert pd.IntervalIndex is native_pd.IntervalIndex
    assert pd.MultiIndex is native_pd.MultiIndex
    assert pd.NamedAgg is native_pd.NamedAgg
    assert pd.NaT is native_pd.NaT
    assert pd.Period is native_pd.Period
    assert pd.PeriodDtype is native_pd.PeriodDtype
    assert pd.PeriodIndex is native_pd.PeriodIndex
    assert pd.RangeIndex is native_pd.RangeIndex
    assert pd.SparseDtype is native_pd.SparseDtype
    assert pd.StringDtype is native_pd.StringDtype
    assert pd.Timedelta is native_pd.Timedelta
    assert pd.Timestamp is native_pd.Timestamp
    assert pd.UInt8Dtype is native_pd.UInt8Dtype
    assert pd.UInt16Dtype is native_pd.UInt16Dtype
    assert pd.UInt32Dtype is native_pd.UInt32Dtype
    assert pd.UInt64Dtype is native_pd.UInt64Dtype
    assert pd.options is native_pd.options
    assert pd.set_option is native_pd.set_option
    # TODO: SNOW-1316523
    # Modin defines its own `modin.pandas.api.extensions` module, which overwrites the attempted re-export
    # of the native `pandas.api` module. Since our `modin.pandas` module follows this
    # structure, we also overwrite this export.
    # assert pd.api is native_pd.api

    # After modin 0.33, re-exported pandas functions are wrapped in a dispatcher to support backend switching
    reexport_functions = [
        "array",
        "eval",
        "factorize",
        "from_dummies",
        "infer_freq",
        "interval_range",
        "period_range",
        "set_eng_float_format",
        "test",
        "timedelta_range",
    ]
    if MODIN_IS_AT_LEAST_0_33_0:
        for name in reexport_functions:
            assert getattr(pd, name)._wrapped_method_for_casting is getattr(
                native_pd, name
            )
        assert pd.array._wrapped_method_for_casting is native_pd.array
    else:
        for name in reexport_functions:
            assert getattr(pd, name) is getattr(native_pd, name)
