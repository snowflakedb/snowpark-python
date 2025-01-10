#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401


@pytest.mark.parametrize(
    "api",
    [
        pd.to_pandas,
        pd.to_snowpark,
        pd.to_snowflake,
    ],
)
def test_wrong_obj(api):
    with pytest.raises(
        TypeError,
        match="obj must be a Snowpark pandas DataFrame or Series",
    ):
        if api.__name__ == "to_snowflake":
            api(native_pd.Series([1, 2, 3]), name="table-name")
        else:
            api(native_pd.Series([1, 2, 3]))
