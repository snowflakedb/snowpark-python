#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import numpy as np
import pandas as native_pd
import pytest

from snowflake.snowpark.modin.plugin._internal.snowpark_pandas_types import (
    TimedeltaType,
)
from snowflake.snowpark.modin.plugin._internal.type_utils import TypeMapper


@pytest.mark.parametrize(
    "pandas_type", [native_pd.Timedelta, np.dtype("timedelta64[ns]")]
)
def test_type_mapper_to_timedelta_SNOW_1635405(pandas_type):
    assert isinstance(TypeMapper.to_snowflake(pandas_type), TimedeltaType)
