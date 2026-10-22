#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

from snowflake.snowpark._internal.document_reader import (
    _CALL_BUDGETS,
    _EXTRACTION_MODEL,
    build_ai_complete_call,
    cast_extracted_field,
    merge_extract_error,
    output_columns,
)
from snowflake.snowpark._internal.document_reader_options import (
    DocumentReaderOptions,
    ExtractionSpec,
)
from snowflake.snowpark.functions import col, lit
from snowflake.snowpark.types import BooleanType, DoubleType, StringType

# _CALL_BUDGETS -- the fixed output-token budgets the split procedure runs
# claude-sonnet-5-5 with.
# ---------------------------------------------------------------------------


class TestCallBudgets:
    def test_the_extraction_model_is_claude_sonnet_5_5(self):
        assert _EXTRACTION_MODEL == "claude-sonnet-5-5"

    def test_budgets(self):
        assert _CALL_BUDGETS == {
            "first": {"target": 80000, "max_tokens": 128000},
            "range": {"target": 120000, "max_tokens": 128000},
            "large_range": {"target": 80000, "max_tokens": 128000},
            "merge_target": 60000,
            "plan_first": {"range_target": 60000, "max_tokens": 64000},
        }


# ---------------------------------------------------------------------------
# cast_extracted_field -- an extracted field's declared JSON-Schema type (if any)
# decides whether the output column is cast, and to what, plus whether there's a
# mismatch condition worth surfacing as an error. No live session is needed:
# cast()/try_cast() build an expression tree in pure Python, so the resulting
# Column can be inspected directly.
# ---------------------------------------------------------------------------


def _spec(field_types: dict) -> ExtractionSpec:
    fields = list(field_types)
    return ExtractionSpec(
        fields=fields,
        field_columns=[f.upper() for f in fields],
        ai_extract_format=None,
        ai_complete_format=None,
        field_types=field_types,
    )


class TestCastExtractedField:
    def test_no_declared_type_returns_the_raw_value_unchanged_with_no_mismatch(self):
        # Natural-language response_format, or a JSON-Schema array/object field:
        # ExtractionSpec.field_types already resolves these to None. No cast is
        # applied at all -- the caller gets back the exact same Column, i.e. the
        # raw VARIANT/ARRAY/OBJECT AI_EXTRACT/AI_COMPLETE returned -- and there's
        # no declared contract to violate, so no mismatch signal either.
        value = col("RESPONSE")["line_items"]
        spec = _spec({"line_items": None})
        result, mismatch = cast_extracted_field(value, "line_items", spec)
        assert result is value
        assert mismatch is None

    def test_declared_string_type_is_a_plain_cast_with_no_mismatch(self):
        # CAST(variant AS VARCHAR) is always defined -- no need for TRY_CAST's
        # NULL-on-failure fallback for a type that can't fail to convert, so
        # there's nothing to detect a mismatch for either.
        value = col("RESPONSE")["name"]
        spec = _spec({"name": StringType()})
        result, mismatch = cast_extracted_field(value, "name", spec)
        expr = result._expression
        assert type(expr).__name__ == "Cast"
        assert expr.try_ is False
        assert expr.to == StringType()
        assert mismatch is None

    def test_declared_numeric_type_is_try_cast_via_string_intermediate(self):
        # TRY_CAST has no VARIANT-source overload (confirmed live against a real
        # account: "Function TRY_CAST cannot be used with arguments of types
        # VARIANT and FLOAT") -- the value must go through CAST(... AS VARCHAR)
        # first. TRY_CAST (not CAST) at the outer layer so a value that doesn't
        # actually match its declared type degrades to NULL instead of aborting
        # the whole read -- the returned mismatch Column is what keeps that
        # degradation from being silent.
        value = col("RESPONSE")["amount"]
        spec = _spec({"amount": DoubleType()})
        result, mismatch = cast_extracted_field(value, "amount", spec)
        outer = result._expression
        assert type(outer).__name__ == "Cast"
        assert outer.try_ is True
        assert outer.to == DoubleType()
        inner = outer.child
        assert type(inner).__name__ == "Cast"
        assert inner.try_ is False
        assert inner.to == StringType()
        assert mismatch is not None

    def test_declared_boolean_type_is_also_try_cast_via_string(self):
        value = col("RESPONSE")["is_paid"]
        spec = _spec({"is_paid": BooleanType()})
        result, mismatch = cast_extracted_field(value, "is_paid", spec)
        outer = result._expression
        assert outer.try_ is True
        assert outer.to == BooleanType()
        assert outer.child.to == StringType()
        assert mismatch is not None


# ---------------------------------------------------------------------------
# merge_extract_error -- folds any field-level type mismatch into the same error
# column AI_EXTRACT/AI_COMPLETE's own in-band error already populates, so a
# mismatch is visible under PERMISSIVE and aborts under FAILFAST like any other
# extraction failure, instead of silently degrading to NULL with no trace.
# ---------------------------------------------------------------------------


class TestMergeExtractError:
    def test_no_mismatch_columns_falls_back_to_the_native_error_untouched(self):
        # Every field either has no declared type or is StringType -- the common
        # case for existing schemas -- so there's nothing to merge in, and the
        # native AI_EXTRACT/AI_COMPLETE error passes through exactly as before.
        native_error = col("EXTRACTED")["error"]
        result = merge_extract_error(native_error, ["NAME"], [None])
        assert type(result._expression).__name__ == "Cast"
        assert result._expression.to == StringType()

    def test_declared_mismatch_columns_are_folded_into_the_error(self):
        # At least one field has a declared scalar type, so there's a real
        # mismatch condition to check -- the result must depend on more than
        # just the native error column now (a plain Cast wouldn't).
        native_error = col("EXTRACTED")["error"]
        mismatch = lit(True)
        result = merge_extract_error(native_error, ["AMOUNT"], [mismatch])
        assert type(result._expression).__name__ != "Cast"


# ---------------------------------------------------------------------------
# build_ai_complete_call -- a prompt-only read sends no response_format.
# ---------------------------------------------------------------------------


class TestBuildAiCompleteCall:
    def test_prompt_only_uses_claude_sonnet_5_5_and_omits_response_format(self):
        options = DocumentReaderOptions(parse_mode="none", prompt="Summarize this.")
        input_col = col("SOURCE_FILE")
        with mock.patch(
            "snowflake.snowpark._internal.document_reader.ai_complete"
        ) as mock_ai_complete:
            build_ai_complete_call(input_col, options)

        mock_ai_complete.assert_called_once()
        args, kwargs = mock_ai_complete.call_args
        assert args[0] == "claude-sonnet-5-5"
        assert args[1] == "Summarize this."
        assert kwargs["file"] is input_col
        assert kwargs["model_parameters"] == {"max_tokens": 128000}
        assert kwargs["response_format"] is None
        assert kwargs["return_error_details"] is True

    def test_schema_present_still_sends_the_fixed_model_and_parameters(self):
        options = DocumentReaderOptions(
            parse_mode="none",
            extraction=ExtractionSpec.from_response_format({"name": "What is it?"}),
        )
        input_col = col("SOURCE_FILE")
        with mock.patch(
            "snowflake.snowpark._internal.document_reader.ai_complete"
        ) as mock_ai_complete:
            build_ai_complete_call(input_col, options)

        args, kwargs = mock_ai_complete.call_args
        assert args[0] == "claude-sonnet-5-5"
        assert kwargs["model_parameters"] == {"max_tokens": 128000}
        assert kwargs["response_format"] == options.extraction.ai_complete_format


# ---------------------------------------------------------------------------
# output_columns -- a prompt-only read (complete_enabled but not extract_enabled)
# has no schema fields to list, so RESPONSE is the one AI_COMPLETE output column
# instead.
# ---------------------------------------------------------------------------


class TestOutputColumnsForPromptOnly:
    def test_prompt_only_outputs_source_file_response_and_corrupt_record(self):
        options = DocumentReaderOptions(parse_mode="none", prompt="Summarize this.")
        assert output_columns(options) == [
            "SOURCE_FILE",
            "RESPONSE",
            '"_document_error"',
        ]
