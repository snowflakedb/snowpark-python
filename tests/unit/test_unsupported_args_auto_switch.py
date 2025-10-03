#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""
Unit tests for auto-switching functionality based on unsupported arguments.
"""

import pytest
from types import MappingProxyType
from unittest.mock import patch, MagicMock

from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    SnowflakeQueryCompiler,
    UnsupportedArgsRule,
    MethodKey,
    HYBRID_SWITCH_FOR_UNSUPPORTED_ARGS,
    HYBRID_SWITCH_FOR_UNIMPLEMENTED_METHODS,
    register_query_compiler_method_not_implemented,
)


class TestHasUnsupportedArgs:
    def setup_method(self):
        HYBRID_SWITCH_FOR_UNSUPPORTED_ARGS.clear()

        HYBRID_SWITCH_FOR_UNSUPPORTED_ARGS[
            MethodKey("BasePandasDataset", "skew")
        ] = UnsupportedArgsRule(
            unsupported_conditions=[
                ("axis", 1),
                (
                    lambda args: args.get("numeric_only") is False,
                    "numeric_only = False argument not supported for skew",
                ),
            ]
        )

        HYBRID_SWITCH_FOR_UNSUPPORTED_ARGS[
            MethodKey(None, "get_dummies")
        ] = UnsupportedArgsRule(
            unsupported_conditions=[
                ("dummy_na", True),
                ("drop_first", True),
                (
                    lambda args: args.get("dtype") is not None,
                    "get_dummies with non-default dtype parameter is not supported yet in Snowpark pandas.",
                ),
            ]
        )

    def test_basic_functionality(self):
        # No operation
        assert not SnowflakeQueryCompiler._has_unsupported_args(
            "DataFrame", None, MappingProxyType({})
        )
        assert not SnowflakeQueryCompiler._has_unsupported_args(
            "DataFrame", "", MappingProxyType({})
        )

        # No args
        assert not SnowflakeQueryCompiler._has_unsupported_args(
            "DataFrame", "method", None
        )

        # No rule registered
        args = MappingProxyType({"param": "value"})
        assert not SnowflakeQueryCompiler._has_unsupported_args(
            "DataFrame", "unknown_method", args
        )

    def test_simple_conditions(self):
        # Match
        args = MappingProxyType({"axis": 1})
        assert SnowflakeQueryCompiler._has_unsupported_args(
            "BasePandasDataset", "skew", args
        )

        # No match
        args = MappingProxyType({"axis": 0, "numeric_only": True})
        assert not SnowflakeQueryCompiler._has_unsupported_args(
            "BasePandasDataset", "skew", args
        )

        # Edge cases
        args = MappingProxyType({})  # Empty args
        assert not SnowflakeQueryCompiler._has_unsupported_args(
            "BasePandasDataset", "skew", args
        )

        args = MappingProxyType({"param": None})  # None values
        HYBRID_SWITCH_FOR_UNSUPPORTED_ARGS[
            MethodKey("DataFrame", "test")
        ] = UnsupportedArgsRule(unsupported_conditions=[("param", None)])
        assert SnowflakeQueryCompiler._has_unsupported_args("DataFrame", "test", args)

    def test_callable_conditions(self):
        # Match
        args = MappingProxyType({"numeric_only": False})
        assert SnowflakeQueryCompiler._has_unsupported_args(
            "BasePandasDataset", "skew", args
        )

        # No match
        args = MappingProxyType({"numeric_only": True})
        assert not SnowflakeQueryCompiler._has_unsupported_args(
            "BasePandasDataset", "skew", args
        )

        # Complex callable condition
        def complex_condition(args):
            return (
                args.get("param1") == "bad"
                and args.get("param2", 0) > 5
                and isinstance(args.get("param3"), list)
            )

        HYBRID_SWITCH_FOR_UNSUPPORTED_ARGS[
            MethodKey("DataFrame", "test")
        ] = UnsupportedArgsRule(
            unsupported_conditions=[(complex_condition, "complex condition failed")]
        )

        # Should match
        args1 = MappingProxyType({"param1": "bad", "param2": 10, "param3": [1, 2, 3]})
        assert SnowflakeQueryCompiler._has_unsupported_args("DataFrame", "test", args1)

        # Should not match
        args2 = MappingProxyType({"param1": "good", "param2": 10, "param3": [1, 2, 3]})
        assert not SnowflakeQueryCompiler._has_unsupported_args(
            "DataFrame", "test", args2
        )

    def test_multiple_conditions(self):
        # One match
        args = MappingProxyType({"dummy_na": True, "drop_first": False})
        assert SnowflakeQueryCompiler._has_unsupported_args(None, "get_dummies", args)

        # No match
        args = MappingProxyType({"dummy_na": False, "drop_first": False})
        assert not SnowflakeQueryCompiler._has_unsupported_args(
            None, "get_dummies", args
        )

    def test_exception_handling(self):
        def failing_condition(args):
            raise ValueError("Test exception")

        HYBRID_SWITCH_FOR_UNSUPPORTED_ARGS[
            MethodKey("DataFrame", "test_method")
        ] = UnsupportedArgsRule(
            unsupported_conditions=[
                (failing_condition, "failing condition"),
                ("valid_param", True),  # This should still be checked
            ]
        )

        args = MappingProxyType({"valid_param": True})

        with patch("logging.warning") as mock_warning:
            result = SnowflakeQueryCompiler._has_unsupported_args(
                "DataFrame", "test_method", args
            )

            # Should return True because the valid condition matches
            assert result
            # Should log the exception
            mock_warning.assert_called_once()


class TestGetUnsupportedArgsReason:
    def setup_method(self):
        HYBRID_SWITCH_FOR_UNSUPPORTED_ARGS.clear()

        HYBRID_SWITCH_FOR_UNSUPPORTED_ARGS[
            MethodKey("BasePandasDataset", "skew")
        ] = UnsupportedArgsRule(
            unsupported_conditions=[
                ("axis", 1),
                (
                    lambda args: args.get("numeric_only") is False,
                    "numeric_only = False argument not supported for skew",
                ),
            ]
        )

    def test_basic_functionality(self):
        # No operation
        assert (
            SnowflakeQueryCompiler._get_unsupported_args_reason(
                "DataFrame", None, MappingProxyType({})
            )
            is None
        )
        assert (
            SnowflakeQueryCompiler._get_unsupported_args_reason(
                "DataFrame", "", MappingProxyType({})
            )
            is None
        )

        # No args
        assert (
            SnowflakeQueryCompiler._get_unsupported_args_reason(
                "DataFrame", "method", None
            )
            is None
        )

        # No rule registered
        args = MappingProxyType({"param": "value"})
        assert (
            SnowflakeQueryCompiler._get_unsupported_args_reason(
                "DataFrame", "unknown_method", args
            )
            is None
        )

    def test_reason_generation(self):
        # Auto-generated reason for simple conditions
        args = MappingProxyType({"axis": 1})
        reason = SnowflakeQueryCompiler._get_unsupported_args_reason(
            "BasePandasDataset", "skew", args
        )
        assert reason == "axis=1 is not supported"

        # Custom reason for callable conditions
        args = MappingProxyType({"numeric_only": False})
        reason = SnowflakeQueryCompiler._get_unsupported_args_reason(
            "BasePandasDataset", "skew", args
        )
        assert reason == "numeric_only = False argument not supported for skew"

        # No matching condition
        args = MappingProxyType({"axis": 0, "numeric_only": True})
        reason = SnowflakeQueryCompiler._get_unsupported_args_reason(
            "BasePandasDataset", "skew", args
        )
        assert reason is None

    def test_first_matching_condition_priority(self):
        HYBRID_SWITCH_FOR_UNSUPPORTED_ARGS[
            MethodKey("DataFrame", "test_method")
        ] = UnsupportedArgsRule(
            unsupported_conditions=[
                (lambda args: args.get("param1") == "bad", "first reason"),
                (
                    lambda args: args.get("param1") == "bad",
                    "second reason",
                ),  # Same condition, different reason
            ]
        )

        args = MappingProxyType({"param1": "bad"})
        reason = SnowflakeQueryCompiler._get_unsupported_args_reason(
            "DataFrame", "test_method", args
        )
        assert reason == "first reason"

    def test_exception_handling(self):
        def failing_condition(args):
            raise ValueError("Test exception")

        HYBRID_SWITCH_FOR_UNSUPPORTED_ARGS[
            MethodKey("DataFrame", "test_method")
        ] = UnsupportedArgsRule(
            unsupported_conditions=[
                (failing_condition, "failing condition reason"),
                (
                    lambda args: args.get("valid_param") is True,
                    "valid condition reason",
                ),
            ]
        )

        args = MappingProxyType({"valid_param": True})

        with patch("logging.warning") as mock_warning:
            reason = SnowflakeQueryCompiler._get_unsupported_args_reason(
                "DataFrame", "test_method", args
            )

            # Should return the reason from the valid condition
            assert reason == "valid condition reason"
            # Should log the exception
            mock_warning.assert_called_once()


class TestRegisterQueryCompilerMethodNotImplemented:
    def setup_method(self):
        HYBRID_SWITCH_FOR_UNSUPPORTED_ARGS.clear()
        HYBRID_SWITCH_FOR_UNIMPLEMENTED_METHODS.clear()

    def test_registration(self):
        rule = UnsupportedArgsRule(unsupported_conditions=[("param", "value")])

        @register_query_compiler_method_not_implemented(
            "DataFrame", "test_method", rule
        )
        def test_method(self):
            return "original_result"

        # Check that rule was registered
        method_key = MethodKey("DataFrame", "test_method")
        assert method_key in HYBRID_SWITCH_FOR_UNSUPPORTED_ARGS
        assert HYBRID_SWITCH_FOR_UNSUPPORTED_ARGS[method_key] == rule

    @patch(
        "modin.core.storage_formats.pandas.query_compiler_caster.register_function_for_pre_op_switch"
    )
    def test_pre_op_switch_registration(self, mock_register):
        rule = UnsupportedArgsRule(unsupported_conditions=[("param", "value")])

        @register_query_compiler_method_not_implemented(
            "DataFrame", "test_method", rule
        )
        def test_method(self):
            return "original_result"

        mock_register.assert_called_once_with(
            class_name="DataFrame", backend="Snowflake", method="test_method"
        )

    def test_decorator_functionality(self):
        rule = UnsupportedArgsRule(unsupported_conditions=[("bad_param", True)])

        @register_query_compiler_method_not_implemented(
            "DataFrame", "test_method", rule
        )
        def test_method(self, param1=None, bad_param=False):
            return "original_result"

        mock_qc = MagicMock()

        # Test with supported args - should work
        result = test_method(mock_qc, param1="good", bad_param=False)
        assert result == "original_result"

        # Test with unsupported args - should raise NotImplementedError
        with pytest.raises(NotImplementedError) as exc_info:
            test_method(mock_qc, param1="good", bad_param=True)

        error_msg = str(exc_info.value)
        expected_msg = (
            "Snowpark pandas test_method does not yet support the parameter combination because bad_param=True is not supported. "
            "Enable auto-switching with 'from modin.config import AutoSwitchBackend; AutoSwitchBackend.enable()' "
            "to use pandas for unsupported operations."
        )
        assert error_msg == expected_msg

    @patch(
        "snowflake.snowpark.modin.plugin.utils.error_message.ErrorMessage.not_implemented_with_reason"
    )
    def test_custom_reason_handling(self, mock_error):
        rule = UnsupportedArgsRule(
            unsupported_conditions=[
                (
                    lambda args: args.get("bad_param") is True,
                    "specific reason for bad_param",
                )
            ]
        )

        @register_query_compiler_method_not_implemented(
            "DataFrame", "test_method", rule
        )
        def test_method(self, bad_param=False):
            return "original_result"

        mock_qc = MagicMock()
        test_method(mock_qc, bad_param=True)

        mock_error.assert_called_once_with(
            "test_method", "specific reason for bad_param"
        )

    def test_fallback_error_case(self):
        # Create a rule that has a condition that matches but doesn't provide a proper reason
        # This simulates a malformed rule where the condition evaluation succeeds but reason extraction fails
        rule = UnsupportedArgsRule(
            unsupported_conditions=[
                ("trigger_param", "bad_value"),  # This will match
            ]
        )

        @register_query_compiler_method_not_implemented(
            "DataFrame", "test_fallback", rule
        )
        def test_fallback(self, trigger_param="safe_value", other_param="default"):
            return f"trigger_param={trigger_param}, other_param={other_param}"

        mock_qc = MagicMock()

        # Patch _get_unsupported_args_reason to return None while _has_unsupported_args returns True
        # This simulates the edge case where condition matching and reason extraction are inconsistent
        with patch.object(
            SnowflakeQueryCompiler, "_get_unsupported_args_reason", return_value=None
        ):
            with pytest.raises(NotImplementedError) as exc_info:
                test_fallback(mock_qc, trigger_param="bad_value")

            error_msg = str(exc_info.value)
            expected_msg_parts = [
                "Snowpark pandas doesn't support 'test_fallback' with the parameter combination:",
                "trigger_param='bad_value'",
                "Enable auto-switching with:",
                "from modin.config import AutoSwitchBackend; AutoSwitchBackend.enable()",
                "to perform these operations in pandas",
            ]

            for part in expected_msg_parts:
                assert (
                    part in error_msg
                ), f"Expected '{part}' to be in error message: {error_msg}"

    def test_parameter_handling_comprehensive(self):
        rule = UnsupportedArgsRule(
            unsupported_conditions=[("bad_arg", "trigger"), ("bad_kwarg", True)]
        )

        @register_query_compiler_method_not_implemented(
            "DataFrame", "test_comprehensive", rule
        )
        def test_comprehensive(
            self,
            pos_arg1="default1",
            pos_arg2="default2",
            bad_arg="safe",
            *,
            bad_kwarg=False,
            **extra_kwargs,
        ):
            return f"pos_arg1={pos_arg1}, pos_arg2={pos_arg2}, bad_arg={bad_arg}, bad_kwarg={bad_kwarg}, extra={extra_kwargs}"

        mock_qc = MagicMock()

        # Default parameters work
        result = test_comprehensive(mock_qc)
        expected = "pos_arg1=default1, pos_arg2=default2, bad_arg=safe, bad_kwarg=False, extra={}"
        assert result == expected

        # Mixed positional and keyword args work
        result = test_comprehensive(
            mock_qc, "custom1", bad_kwarg=False, extra_param="extra_value"
        )
        expected = "pos_arg1=custom1, pos_arg2=default2, bad_arg=safe, bad_kwarg=False, extra={'extra_param': 'extra_value'}"
        assert result == expected

        # Bad positional arg triggers error
        with pytest.raises(NotImplementedError) as exc_info:
            test_comprehensive(mock_qc, "custom1", "custom2", "trigger")
        assert "bad_arg='trigger' is not supported" in str(exc_info.value)

        # Bad keyword arg triggers error
        with pytest.raises(NotImplementedError) as exc_info:
            test_comprehensive(mock_qc, bad_kwarg=True)
        assert "bad_kwarg=True is not supported" in str(exc_info.value)

        # Extra kwargs work fine
        result = test_comprehensive(mock_qc, extra1="value1", extra2="value2")
        expected = "pos_arg1=default1, pos_arg2=default2, bad_arg=safe, bad_kwarg=False, extra={'extra1': 'value1', 'extra2': 'value2'}"
        assert result == expected

    def test_special_argument_types(self):
        # Positional-only arguments
        rule = UnsupportedArgsRule(
            unsupported_conditions=[("trigger_param", "bad_value")]
        )

        @register_query_compiler_method_not_implemented(
            "DataFrame", "test_positional_only", rule
        )
        def test_positional_only(
            self,
            pos_only1,
            pos_only2="default_pos",
            trigger_param="safe_value",
            /,
            normal_param="normal_default",
        ):
            return f"pos_only1={pos_only1}, pos_only2={pos_only2}, trigger_param={trigger_param}, normal_param={normal_param}"

        mock_qc = MagicMock()

        # Works with defaults
        result = test_positional_only(mock_qc, "required_value")
        expected = "pos_only1=required_value, pos_only2=default_pos, trigger_param=safe_value, normal_param=normal_default"
        assert result == expected

        # Triggers error with bad positional arg
        with pytest.raises(NotImplementedError) as exc_info:
            test_positional_only(mock_qc, "required_value", "custom_pos", "bad_value")
        assert "trigger_param='bad_value' is not supported" in str(exc_info.value)

        # Keyword-only arguments
        rule2 = UnsupportedArgsRule(unsupported_conditions=[("kw_only_bad", "trigger")])

        @register_query_compiler_method_not_implemented(
            "DataFrame", "test_keyword_only", rule2
        )
        def test_keyword_only(
            self,
            normal_param="normal_default",
            *,
            kw_only_good="kw_default",
            kw_only_bad="safe",
        ):
            return f"normal_param={normal_param}, kw_only_good={kw_only_good}, kw_only_bad={kw_only_bad}"

        # Works with defaults
        result = test_keyword_only(mock_qc)
        expected = (
            "normal_param=normal_default, kw_only_good=kw_default, kw_only_bad=safe"
        )
        assert result == expected

        # Triggers error with bad keyword-only arg
        with pytest.raises(NotImplementedError) as exc_info:
            test_keyword_only(mock_qc, kw_only_bad="trigger")
        assert "kw_only_bad='trigger' is not supported" in str(exc_info.value)


class TestMalformedConditions:
    def setup_method(self):
        HYBRID_SWITCH_FOR_UNSUPPORTED_ARGS.clear()

    def test_malformed_condition_formats(self):
        malformed_rule = UnsupportedArgsRule(
            unsupported_conditions=[
                ("single_item",),  # Wrong length
                ("param1", "value1", "extra_item"),  # Too many items
                "not_a_tuple",  # Invalid: not a tuple
                ["list", "instead"],  # Invalid: list instead of tuple
                (123, "not_string_arg_name"),  # Invalid: non-string arg name
                ("param2", "value2"),  # Valid condition
            ]
        )
        HYBRID_SWITCH_FOR_UNSUPPORTED_ARGS[
            MethodKey("DataFrame", "malformed_test")
        ] = malformed_rule

        args = MappingProxyType({"param2": "value2"})

        with patch("logging.warning") as mock_warning:
            # Should still work with the valid condition
            result = SnowflakeQueryCompiler._has_unsupported_args(
                "DataFrame", "malformed_test", args
            )
            assert result

            # Should get auto-generated reason from valid condition
            reason = SnowflakeQueryCompiler._get_unsupported_args_reason(
                "DataFrame", "malformed_test", args
            )
            assert reason == "param2='value2' is not supported"

            # Should log warnings for malformed conditions during evaluation, not during instantiation
            assert (
                mock_warning.call_count >= 4
            )  # At least 4 malformed conditions during evaluation

    def test_none_conditions(self):
        empty_rule = UnsupportedArgsRule(
            unsupported_conditions=[]
        )  # Empty conditions list
        HYBRID_SWITCH_FOR_UNSUPPORTED_ARGS[
            MethodKey("DataFrame", "empty_test")
        ] = empty_rule

        args = MappingProxyType({"any_param": "any_value"})
        assert not SnowflakeQueryCompiler._has_unsupported_args(
            "DataFrame", "empty_test", args
        )
        assert (
            SnowflakeQueryCompiler._get_unsupported_args_reason(
                "DataFrame", "empty_test", args
            )
            is None
        )

        # None values in condition tuple
        none_rule = UnsupportedArgsRule(
            unsupported_conditions=[
                (None, "reason_for_none"),  # None as first element
                ("param_name", None),  # None as second element
            ]
        )
        HYBRID_SWITCH_FOR_UNSUPPORTED_ARGS[
            MethodKey("DataFrame", "none_test")
        ] = none_rule

        args = MappingProxyType({"param_name": None, "valid_param": "value"})

        with patch("logging.warning") as mock_warning:
            result = SnowflakeQueryCompiler._has_unsupported_args(
                "DataFrame", "none_test", args
            )
            assert result  # Should match the None value condition

            reason = SnowflakeQueryCompiler._get_unsupported_args_reason(
                "DataFrame", "none_test", args
            )
            assert reason == "param_name=None is not supported"

            # Should log warning for the None as first element condition during evaluation
            assert mock_warning.call_count >= 1
