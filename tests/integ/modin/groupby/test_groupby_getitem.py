#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pandas.core.groupby.generic import (
    DataFrameGroupBy as native_df_groupby,
    SeriesGroupBy as native_ser_groupby,
)

import snowflake.snowpark.modin.plugin  # noqa: F401
from modin.pandas.groupby import (
    DataFrameGroupBy as snow_df_groupby,
    SeriesGroupBy as snow_ser_groupby,
)
from tests.integ.utils.sql_counter import sql_count_checker

data_dictionary = {
    "col1_grp": ["g1", "g2", "g0", "g0", "g2", "g3", "g0", "g2", "g3"],
    "col2_int64": np.arange(9, dtype="int64") // 3,
    "col3_int_identical": [2] * 9,
    "col4_int32": np.arange(9, dtype="int32") // 4,
    "col5_int16": np.arange(9, dtype="int16") // 3,
}


def check_groupby_types_same(native_groupby, snow_groupby):
    if isinstance(native_groupby, native_df_groupby):
        assert isinstance(snow_groupby, snow_df_groupby)
    elif isinstance(native_groupby, native_ser_groupby):
        assert isinstance(snow_groupby, snow_ser_groupby)
    else:
        raise ValueError(
            f"Unknown GroupBy type for native pandas: {type(native_groupby)}. Snowpark pandas GroupBy type: {type(snow_groupby)}"
        )


@pytest.mark.parametrize(
    "by",
    [
        "col1_grp",
        ["col1_grp", "col2_int64"],
        ["col1_grp", "col2_int64", "col3_int_identical"],
    ],
)
@pytest.mark.parametrize("as_index", [True, False])
@pytest.mark.parametrize("indexer", ["col5_int16", ["col5_int16", "col4_int32"]])
@sql_count_checker(query_count=0)
def test_groupby_getitem(by, as_index, indexer):
    snow_df = pd.DataFrame(data_dictionary)
    native_df = native_pd.DataFrame(data_dictionary)
    check_groupby_types_same(
        native_df.groupby(by=by, as_index=as_index)[indexer],
        snow_df.groupby(by, as_index=as_index)[indexer],
    )
