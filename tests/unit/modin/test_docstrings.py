#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd

import snowflake.snowpark.modin.plugin  # noqa: F401


def test_base_property_snow_1305329():
    assert "Snowpark pandas" in pd.base.BasePandasDataset.iloc.__doc__
