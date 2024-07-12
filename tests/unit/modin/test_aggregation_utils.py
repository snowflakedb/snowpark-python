#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import numpy as np
import pytest

from snowflake.snowpark.modin.plugin._internal.aggregation_utils import (
    check_is_aggregation_supported_in_snowflake,
    is_supported_snowflake_agg_func,
)


@pytest.mark.parametrize(
    "agg_func, agg_kwargs, axis, is_valid",
    [
        (np.sum, {}, 0, True),
        (np.sum, {}, 1, True),
        (np.mean, {}, 0, True),
        (np.mean, {}, 1, False),
        (np.median, {}, 0, True),
        (np.median, {}, 1, False),
        (np.max, {}, 0, True),
        (np.max, {}, 1, True),
        (np.min, {}, 0, True),
        (np.min, {}, 1, True),
        ("median", {}, 0, True),
        ("median", {}, 1, False),
        ("max", {}, 0, True),
        ("max", {}, 1, True),
        ("count", {}, 0, True),
        ("count", {}, 1, True),
        ("min", {}, 0, True),
        ("min", {}, 1, True),
        ("test", {}, 0, False),
        ("test", {}, 1, False),
        (np.random, {}, 0, False),
        (np.random, {}, 1, False),
        (np.std, {}, 0, True),
        (np.std, {}, 1, False),
        ("std", {"ddof": 0}, 0, True),
        ("std", {"ddof": 0}, 1, False),
        ("std", {"ddof": 4}, 0, False),
        ("std", {"ddof": 4}, 1, False),
        (np.var, {"ddof": 1}, 0, True),
        (np.var, {"ddof": 1}, 1, False),
        ("var", {"ddof": 5}, 0, False),
        ("var", {"ddof": 5}, 1, False),
        ("quantile", {}, 0, True),
        ("quantile", {"q": [0.1, 0.2]}, 0, False),
        ("quantile", {"interpolation": "nearest"}, 0, True),
        ("quantile", {"interpolation": "midpoint"}, 0, False),
        ("quantile", {}, 1, False),
    ],
)
def test_is_supported_snowflake_agg_func(agg_func, agg_kwargs, axis, is_valid) -> None:
    assert is_supported_snowflake_agg_func(agg_func, agg_kwargs, axis) is is_valid


@pytest.mark.parametrize(
    "agg_func, agg_kwargs, expected_result",
    [
        ("max", {}, True),  # snowflake supported aggregation function str
        (np.sum, {}, True),  # snowflake supported aggregation function numpy function
        (np.quantile, {}, False),  # snowflake unsupported aggregation function
        (
            {"col1": "max", "col2": np.sum},
            {},
            True,
        ),  # dictionary fo aggregation functions
        ({"col1": np.min, "col2": ["max", "sum"]}, {}, True),
        ({"col1": np.quantile, "col2": [np.mean, "max"]}, {}, False),
        ({"col1": "max", "col2": ["min", np.quantile, "max"]}, {}, False),
        ([np.min, "max", "max", np.sum], {}, True),
        ([np.percentile, min, sum, "min"], {}, False),
        ("std", {}, True),  # std with no ddof configured (default 1)
        ("std", {"ddof": 0}, True),  # std with ddof 0
        ("std", {"ddof": 10}, False),  # std with ddof 10
        ("var", {"ddof": 1}, True),  # var with ddof 1
        ("var", {"ddof": 5}, False),  # var with ddof 5
        (["var", "max", "std"], {"ddof": 1}, True),
        (
            "quantile",
            {},
            True,
        ),  # quantile with no interpolation (default "linear") and no quantile (default 0.5)
        (
            "quantile",
            {"q": [0.1, 0.2]},
            False,
        ),  # agg("quantile") with list q is unsupported because result has multiple rows
        ("quantile", {"interpolation": "linear"}, True),
        ("quantile", {"interpolation": "nearest"}, True),
        ("quantile", {"interpolation": "lower"}, False),
        ("quantile", {"interpolation": "higher"}, False),
        ("quantile", {"interpolation": "midpoint"}, False),
    ],
)
def test_check_aggregation_snowflake_execution_capability_by_args(
    agg_func, agg_kwargs, expected_result
):
    can_be_distributed = check_is_aggregation_supported_in_snowflake(
        agg_func=agg_func, agg_kwargs=agg_kwargs, axis=0
    )
    assert can_be_distributed == expected_result
