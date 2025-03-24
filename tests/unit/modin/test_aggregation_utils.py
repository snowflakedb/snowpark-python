#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from types import MappingProxyType
from unittest import mock

import numpy as np
import pytest

import snowflake.snowpark.modin.plugin._internal.aggregation_utils as aggregation_utils
from snowflake.snowpark.functions import greatest, sum as sum_
from snowflake.snowpark.modin.plugin._internal.aggregation_utils import (
    SnowflakeAggFunc,
    _are_all_agg_funcs_supported_by_snowflake,
    _is_supported_snowflake_agg_func,
    _SnowparkPandasAggregation,
    check_is_aggregation_supported_in_snowflake,
    get_snowflake_agg_func,
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
        ("size", {}, 0, True),
        ("size", {}, 1, True),
        (sum, {}, 0, True),
        (len, {}, 0, True),
        (len, {}, 1, True),
        (min, {}, 0, True),
        (max, {}, 0, True),
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
def test__is_supported_snowflake_agg_func(agg_func, agg_kwargs, axis, is_valid) -> None:
    (
        is_supported,
        unsupported_func,
        unsupported_kwargs,
    ) = _is_supported_snowflake_agg_func(agg_func, agg_kwargs, axis)
    assert is_supported == is_valid


@pytest.mark.parametrize(
    "agg_func, agg_kwargs, axis, is_valid",
    [
        ([np.sum, np.mean], {}, 0, True),
        ([np.max, np.median], {}, 1, False),
        ([np.max, np.min], {}, 0, True),
        (["median", "max", "count"], {}, 0, True),
        (["size", "max", "sum"], {}, 0, True),
        (["test", "max", "sum"], {}, 0, False),
        (["std", "max", "sum"], {"ddof": 0}, 0, True),
    ],
)
def test__are_all_agg_funcs_supported_by_snowflake(
    agg_func, agg_kwargs, axis, is_valid
):
    (
        is_supported,
        unsupported_func,
        unsupported_kwargs,
    ) = _are_all_agg_funcs_supported_by_snowflake(agg_func, agg_kwargs, axis)
    assert is_supported == is_valid


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
    (
        can_be_distributed,
        unsupported_arguments,
        is_supported_kwargs,
    ) = check_is_aggregation_supported_in_snowflake(
        agg_func=agg_func, agg_kwargs=agg_kwargs, axis=0
    )
    assert can_be_distributed == expected_result


@pytest.mark.parametrize(
    "agg_func, agg_kwargs, axis, expected",
    [
        (np.sum, {}, 0, SnowflakeAggFunc(sum_, True, supported_in_pivot=True)),
        (
            "max",
            {"skipna": False},
            1,
            SnowflakeAggFunc(greatest, True, supported_in_pivot=True),
        ),
        ("test", {}, 0, None),
    ],
)
def test_get_snowflake_agg_func(agg_func, agg_kwargs, axis, expected):
    result = get_snowflake_agg_func(agg_func, agg_kwargs, axis)
    if expected is None:
        assert result is None
    else:
        assert result == expected


def test_get_snowflake_agg_func_with_no_implementation_on_axis_0():
    """Test get_snowflake_agg_func for a function that we support on axis=1 but not on axis=0."""
    # We have to patch the internal dictionary
    # _PANDAS_AGGREGATION_TO_SNOWPARK_PANDAS_AGGREGATION here because there is
    # no real function that we support on axis=1 but not on axis=0.
    with mock.patch.object(
        aggregation_utils,
        "_PANDAS_AGGREGATION_TO_SNOWPARK_PANDAS_AGGREGATION",
        MappingProxyType(
            {
                "max": _SnowparkPandasAggregation(
                    preserves_snowpark_pandas_types=True,
                    axis_1_aggregation_keepna=greatest,
                    axis_1_aggregation_skipna=greatest,
                    supported_in_pivot=True,
                )
            }
        ),
    ):
        assert get_snowflake_agg_func(agg_func="max", agg_kwargs={}, axis=0) is None
