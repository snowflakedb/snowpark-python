#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))


def test_kwargs_based_auto_switching():
    """
    Test kwargs-based auto-switching with cumsum method frontend override.

    This test validates that the enhanced decorator approach works for methods
    with parameter limitations, demonstrating cost-based backend switching.
    """
    try:
        from tests.parameters import CONNECTION_PARAMETERS
        from snowflake.snowpark import Session
        import modin.pandas as pd
        from modin.config import context as config_context
        from types import MappingProxyType
        from modin.core.storage_formats.base.query_compiler import QCCoercionCost

        from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
            SnowflakeQueryCompiler,
            HYBRID_SWITCH_FOR_UNSUPPORTED_PARAMS,
        )

        session = Session.builder.configs(CONNECTION_PARAMETERS).create()
        pd.session = session

        # Verify kwargs rule registration
        cumsum_rule = HYBRID_SWITCH_FOR_UNSUPPORTED_PARAMS.get(
            ("BasePandasDataset", "cumsum")
        )
        assert cumsum_rule is not None, "cumsum rule should be registered"
        assert (
            len(cumsum_rule.unsupported_conditions) > 0
        ), "axis condition should be registered"

        # Test kwargs detection logic
        args_axis0 = MappingProxyType({"axis": 0, "skipna": True})
        args_axis1 = MappingProxyType({"axis": 1, "skipna": True})

        should_switch_axis0 = SnowflakeQueryCompiler._has_unsupported_kwargs(
            "BasePandasDataset", "cumsum", args_axis0
        )
        should_switch_axis1 = SnowflakeQueryCompiler._has_unsupported_kwargs(
            "BasePandasDataset", "cumsum", args_axis1
        )

        assert should_switch_axis0 is False, "axis=0 should not trigger switching"
        assert should_switch_axis1 is True, "axis=1 should trigger switching"

        # Test cost calculation integration
        df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})

        cost_axis0 = SnowflakeQueryCompiler.move_to_me_cost(
            df._query_compiler,
            api_cls_name="BasePandasDataset",
            operation="cumsum",
            arguments=args_axis0,
        )

        cost_axis1 = SnowflakeQueryCompiler.move_to_me_cost(
            df._query_compiler,
            api_cls_name="BasePandasDataset",
            operation="cumsum",
            arguments=args_axis1,
        )

        assert (
            cost_axis0 != QCCoercionCost.COST_IMPOSSIBLE
        ), "axis=0 should allow movement to Snowflake"

        assert (
            cost_axis1 == QCCoercionCost.COST_IMPOSSIBLE
        ), "axis=1 should prevent movement to Snowflake"

        # Test method execution
        result_axis0 = df.cumsum(axis=0)
        expected_axis0 = [[1, 4], [3, 9], [6, 15]]
        assert (
            result_axis0.values.tolist() == expected_axis0
        ), "axis=0 result should be correct"

        # Test auto-switching behavior
        snow_df = df.move_to("Snowflake")
        assert snow_df.get_backend() == "Snowflake", "DataFrame should be on Snowflake"

        stay_cost_axis1 = snow_df._query_compiler.stay_cost(
            api_cls_name="BasePandasDataset", operation="cumsum", arguments=args_axis1
        )
        assert (
            stay_cost_axis1 == QCCoercionCost.COST_IMPOSSIBLE
        ), "Should prevent staying on Snowflake"

        # Test error message when auto-switching is disabled
        with config_context(AutoSwitchBackend=False):
            try:
                snow_df.cumsum(axis=1)
                raise AssertionError("Should have raised NotImplementedError")
            except NotImplementedError as e:
                error_msg = str(e)
                # print(f"Error message when auto-switching disabled: {error_msg}")

                assert (
                    "is not supported" in error_msg
                ), f"Error should mention 'is not supported': {error_msg}"
                assert (
                    "axis=1" in error_msg
                ), f"Error should mention 'axis=1': {error_msg}"

        # Test auto-switching behavior when enabled
        with config_context(AutoSwitchBackend=True):
            result_axis1 = snow_df.cumsum(axis=1)
            expected_axis1 = [[1, 5], [2, 7], [3, 9]]

            assert (
                result_axis1.get_backend() == "Pandas"
            ), "Should auto-switch to Pandas backend"
            assert (
                result_axis1.values.tolist() == expected_axis1
            ), "axis=1 result should be correct"

        session.close()
        return True

    except Exception:
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_kwargs_based_auto_switching()
    sys.exit(0 if success else 1)
