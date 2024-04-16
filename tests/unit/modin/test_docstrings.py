#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark.modin.pandas.base import BasePandasDataset


def test_base_property_snow_1305329():
    assert "Snowpark pandas" in BasePandasDataset.iloc.__doc__
