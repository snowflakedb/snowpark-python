#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest

agg_func_supported_for_timedelta = ("count",)
agg_func_not_supported_for_timedelta = (
    "sum",
    "mean",
    "var",
    "std",
    "min",
    "max",
    "sem",
)
agg_func = pytest.mark.parametrize(
    "agg_func",
    (*agg_func_supported_for_timedelta, *agg_func_not_supported_for_timedelta),
)
