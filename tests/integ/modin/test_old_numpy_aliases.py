#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pytest
from packaging import version

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import sql_count_checker

# As of writing, Snowpark pandas does not currently support numpy 2.x, but we have already
# excised the np.NaN and np.float_ aliases from our codebase. When we eventually do support
# numpy 2.x, we want to ensure that we remain compatible with these aliases for users
# that are using older versions of numpy.
np_version_guard = pytest.mark.skipif(
    version.parse(np.__version__) >= version.parse("2.0.0"),
    reason="numpy<2 required to test old aliases",
)


@np_version_guard
@sql_count_checker(query_count=1)
def test_old_nan():
    # Check the np.NaN alias (removed in 2.x in favor of np.nan) still works
    s = pd.Series([0.1, np.NaN])
    mask = s.isnull().to_pandas()
    assert not mask[0] and mask[1]


@np_version_guard
@sql_count_checker(query_count=0)
def test_float_conversion():
    # Check that np.float_ is properly converted to/from Snowpark
    s = pd.Series([0.1], dtype=np.float_, name="data")
    assert s.dtype == np.float64
    snowpark_df = s.to_snowpark(index_label="idx")
    assert dict(snowpark_df.dtypes) == {
        '"idx"': "bigint",
        '"data"': "double",
    }
