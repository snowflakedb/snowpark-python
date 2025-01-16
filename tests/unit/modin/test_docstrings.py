#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd

import snowflake.snowpark.modin.plugin as plugin


def test_base_property_snow_1305329():
    assert "Snowpark pandas" in pd.base.BasePandasDataset.iloc.__doc__


def test_series_utils_snow_1461192():
    # Checks that StringMethods and DatetimeProperties correctly inherit documentation
    # Series.dt.date differs from pandas because we don't support DatetimeIndex
    assert (
        pd.series_utils.DatetimeProperties.date.__doc__
        != native_pd.core.indexes.accessors.DatetimeProperties.date.__doc__
    )
    assert (
        pd.series_utils.DatetimeProperties.date.__doc__
        == plugin.docstrings.CombinedDatetimelikeProperties.date.__doc__
    )
    # Series.str.split differs from pandas because we don't yet support the `regex` arg
    assert (
        pd.series_utils.StringMethods.split.__doc__
        != native_pd.core.strings.accessor.StringMethods.split.__doc__
    )
    assert (
        pd.series_utils.StringMethods.split.__doc__
        == plugin.docstrings.StringMethods.split.__doc__
    )
