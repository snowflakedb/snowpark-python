#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark.modin.pandas.api.extensions import (
    register_dataframe_accessor,
    register_series_accessor,
)
from snowflake.snowpark.modin.pandas.base import BasePandasDataset

# Methods that, for whatever reason, need to be overridden in both Series and DF.

methods = ["_binary_op", "resample", "expanding", "rolling", "_aggregate"]

for method in methods:
    base_method = getattr(BasePandasDataset, method)
    # pd.base.BasePandasDataset.method = base_method
    register_dataframe_accessor(method)(base_method)
    register_series_accessor(method)(base_method)
