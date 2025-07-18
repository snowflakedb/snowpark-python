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
        pd.to_view,
        pd.to_dynamic_table,
        pd.to_iceberg,
    ],
)
def test_wrong_obj(api):
    with pytest.raises(
        TypeError,
        match="obj must be a Snowpark pandas DataFrame or Series",
    ):
        if api.__name__ in ["to_snowflake", "to_view"]:
            api(native_pd.Series([1, 2, 3]), name="object-name")
        elif api.__name__ == "to_dynamic_table":
            api(
                native_pd.Series([1, 2, 3]),
                name="object-name",
                warehouse="warehouse",
                lag="lag",
            )
        elif api.__name__ == "to_iceberg":
            api(
                native_pd.Series([1, 2, 3]), table_name="object-name", iceberg_config={}
            )
        else:
            api(native_pd.Series([1, 2, 3]))
