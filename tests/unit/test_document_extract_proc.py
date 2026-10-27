#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import copy
import hashlib
import io
import json
import sys
import types

import pytest

import snowflake.snowpark._internal.document_extract_proc as proc
from snowflake.snowpark._internal.document_extract_proc import (
    BOUNDED_FIRST_CALL_MIN_BYTES,
    BOUNDED_FIRST_CALL_MIN_PAGES,
    EMPTY_ANSWER_ERROR,
    HARD_MAX_CALLS_PER_FILE,
    MAX_FAILED_FRACTION,
    MAX_FILE_BYTES,
    MAX_FILE_READ_BYTES,
    MAX_THROTTLE_RETRIES,
    MIN_DOCUMENTS_BEFORE_ABORT,
    SCORES_FIELD,
    STATUS_FIELD,
    THROTTLE_BACKOFF_SECONDS,
    TRUNCATED_TEXT_ERROR,
    DocumentExtraction,
    FileState,
    NoModelAvailable,
    _brackets_close,
    _cover,
    _to_document_page,
    abort_message,
    build_plan_predicate,
    build_predicate,
    call_cap,
    coalesce_ranges,
    continues_across_split,
    document_estimate,
    document_outcomes,
    drop_unsupported,
    first_round_budget,
    initial_ranges,
    is_empty_answer,
    is_splittable_error,
    is_throttle_error,
    is_unknown_model_error,
    listed_size,
    merge_scores,
    merge_values,
    next_ranges,
    no_model_message,
    nullable_scalars,
    parse_text_answer,
    planning_segments,
    qualify_listed_name,
    range_budget,
    ranges_from_plan,
    ranges_from_salvaged_status,
    read_answer,
    result_row,
    salvage_status,
    schema_instruction,
    scores_schema,
    shift_page_fields,
    should_abort,
    split_evenly,
    stop_waiting,
    unwrap_envelope,
    with_status,
)


def _contains_required(value) -> bool:
    if isinstance(value, dict):
        return "required" in value or any(_contains_required(v) for v in value.values())
    if isinstance(value, list):
        return any(_contains_required(v) for v in value)
    return False


# ---------------------------------------------------------------------------
# drop_unsupported -- strips JSON Schema keywords Cortex json mode rejects or
# enforces too strictly (required, additionalProperties, pattern, format,
# etc.) at every depth -- EXCEPT where the same spelling is a field name
# rather than a keyword, i.e. a key directly inside a name map (properties,
# $defs, definitions, patternProperties), which is never filtered.
# ---------------------------------------------------------------------------


class TestDropUnsupported:
    def test_removes_an_unsupported_keyword_from_a_flat_object(self):
        schema = {
            "type": "object",
            "required": ["a"],
            "properties": {"a": {"type": "string"}},
        }
        assert drop_unsupported(schema) == {
            "type": "object",
            "properties": {"a": {"type": "string"}},
        }

    def test_removes_unsupported_keywords_at_every_nesting_level(self):
        schema = {
            "type": "object",
            "required": ["x"],
            "properties": {
                "x": {
                    "type": "object",
                    "required": ["y"],
                    "properties": {"y": {"type": "string"}},
                }
            },
        }
        assert drop_unsupported(schema) == {
            "type": "object",
            "properties": {
                "x": {"type": "object", "properties": {"y": {"type": "string"}}}
            },
        }

    def test_removes_unsupported_keywords_inside_an_array_items_schema(self):
        schema = {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["a", "b"],
                "properties": {"a": {"type": "string"}, "b": {"type": "integer"}},
            },
        }
        assert drop_unsupported(schema) == {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {"a": {"type": "string"}, "b": {"type": "integer"}},
            },
        }

    def test_additional_properties_is_removed(self):
        schema = {
            "type": "object",
            "additionalProperties": False,
            "properties": {"a": {"type": "string"}},
        }
        assert drop_unsupported(schema) == {
            "type": "object",
            "properties": {"a": {"type": "string"}},
        }

    def test_a_field_named_like_a_keyword_survives_in_properties(self):
        # The properties are named "format" and "default" -- those names are
        # never filtered -- but the "format"/"default" keyword inside each of
        # their own subschemas is.
        schema = {
            "type": "object",
            "properties": {
                "format": {"type": "string", "format": "date"},
                "default": {"type": "integer", "default": 5},
            },
        }
        assert drop_unsupported(schema) == {
            "type": "object",
            "properties": {
                "format": {"type": "string"},
                "default": {"type": "integer"},
            },
        }

    def test_a_defs_entry_named_like_a_keyword_survives(self):
        schema = {"$defs": {"required": {"type": "string", "pattern": "^[A-Z]+$"}}}
        assert drop_unsupported(schema) == {"$defs": {"required": {"type": "string"}}}

    def test_recurses_into_a_bare_list(self):
        value = [{"required": ["a"], "b": 1}, {"c": 2}]
        assert drop_unsupported(value) == [{"b": 1}, {"c": 2}]

    @pytest.mark.parametrize("value", ["hello", 42, True, None, 3.14])
    def test_non_dict_non_list_values_pass_through_unchanged(self, value):
        assert drop_unsupported(value) == value

    def test_does_not_mutate_the_input(self):
        schema = {
            "type": "object",
            "required": ["a"],
            "properties": {"a": {"type": "string"}},
        }
        original = copy.deepcopy(schema)
        drop_unsupported(schema)
        assert schema == original


# ---------------------------------------------------------------------------
# is_splittable_error -- only an error that fewer pages can fix is worth
# retrying with a smaller page range: a response cut off mid-JSON (hit
# max_tokens); an input Cortex rejects as too large (reported only as
# "malformed request"); an empty object (see is_empty_answer and
# EMPTY_ANSWER_ERROR below); or an answer that does not parse as JSON. Any
# other error is not.
# ---------------------------------------------------------------------------


class TestIsSplittableError:
    def test_true_when_the_message_is_exactly_the_truncation_signature(self):
        assert is_splittable_error("unexpected end of JSON input") is True

    def test_true_when_the_truncation_signature_is_a_substring(self):
        assert (
            is_splittable_error(
                "JSONDecodeError: unexpected end of JSON input at position 42"
            )
            is True
        )

    def test_true_when_the_message_is_exactly_the_malformed_request_signature(self):
        assert is_splittable_error("malformed request") is True

    def test_true_when_the_malformed_request_signature_is_a_substring(self):
        assert (
            is_splittable_error("Cortex error: malformed request for this file") is True
        )

    def test_true_when_the_message_is_exactly_the_empty_answer_signature(self):
        assert is_splittable_error(EMPTY_ANSWER_ERROR) is True

    def test_true_when_the_empty_answer_signature_is_a_substring(self):
        assert is_splittable_error(f"pages 1-10: {EMPTY_ANSWER_ERROR}") is True

    def test_false_for_other_errors(self):
        assert is_splittable_error("rate limited") is False
        assert is_splittable_error("") is False


# ---------------------------------------------------------------------------
# is_empty_answer -- json mode can return an empty-ish object (every field
# absent) instead of an error when the model's answer would not fit; run()
# turns that into EMPTY_ANSWER_ERROR so next_ranges treats it like a
# splittable error instead of a silently "complete" empty result.
# ---------------------------------------------------------------------------


class TestIsEmptyAnswer:
    def test_an_empty_dict_is_empty(self):
        assert is_empty_answer({}) is True

    def test_a_dict_of_only_absent_values_is_empty(self):
        assert is_empty_answer({"a": None, "b": "", "c": [], "d": {}}) is True

    def test_a_dict_with_any_present_value_is_not_empty(self):
        assert is_empty_answer({"a": 1, "b": None}) is False

    @pytest.mark.parametrize("value", [None, "not a dict", 42, []])
    def test_non_dict_values_are_not_empty(self, value):
        assert is_empty_answer(value) is False


# ---------------------------------------------------------------------------
# first_round_budget -- a big first call (hundreds of pages, or near the FILE
# byte limit) is more likely to need splitting anyway, so it is planned
# straight to the small "first" budget; below both thresholds a whole answer
# usually fits in one call, so the bigger "range" budget is used instead.
# ---------------------------------------------------------------------------


class TestFirstRoundBudget:
    def test_small_document_gets_the_range_budget(self):
        assert first_round_budget(10, 1_000_000) == "range"

    def test_more_pages_than_the_threshold_gets_the_first_budget(self):
        assert (
            first_round_budget(BOUNDED_FIRST_CALL_MIN_PAGES + 1, 1_000_000) == "first"
        )

    def test_exactly_the_page_threshold_gets_the_range_budget(self):
        # Strictly greater than the threshold is required.
        assert first_round_budget(BOUNDED_FIRST_CALL_MIN_PAGES, 1_000_000) == "range"

    def test_more_bytes_than_the_threshold_gets_the_first_budget(self):
        assert first_round_budget(10, BOUNDED_FIRST_CALL_MIN_BYTES + 1) == "first"

    def test_exactly_the_byte_threshold_gets_the_range_budget(self):
        # Strictly greater than the threshold is required.
        assert first_round_budget(10, BOUNDED_FIRST_CALL_MIN_BYTES) == "range"

    def test_over_both_thresholds_gets_the_first_budget(self):
        assert (
            first_round_budget(
                BOUNDED_FIRST_CALL_MIN_PAGES + 1, BOUNDED_FIRST_CALL_MIN_BYTES + 1
            )
            == "first"
        )


# ---------------------------------------------------------------------------
# range_budget -- a large file's planned ranges use "large_range", so they are
# planned and run against the same smaller target; every other file keeps "range".
# ---------------------------------------------------------------------------


class TestRangeBudget:
    def test_small_file_keeps_the_range_budget(self):
        assert range_budget(10, 1_000_000) == "range"

    def test_more_pages_than_the_threshold_gets_the_large_range_budget(self):
        assert (
            range_budget(BOUNDED_FIRST_CALL_MIN_PAGES + 1, 1_000_000) == "large_range"
        )

    def test_more_bytes_than_the_threshold_gets_the_large_range_budget(self):
        assert range_budget(10, BOUNDED_FIRST_CALL_MIN_BYTES + 1) == "large_range"


# ---------------------------------------------------------------------------
# initial_ranges -- page ranges for the first round of calls: the whole file
# when it already fits in one call, otherwise even ranges sized from the
# average page size so each part (after the first, which also carries one
# context page) stays under the byte limit Cortex accepts for a FILE input.
# ---------------------------------------------------------------------------


class TestInitialRanges:
    def test_file_within_the_byte_limit_is_a_single_range_at_any_page_count(self):
        assert initial_ranges(66, 216_000) == [(1, 66)]
        assert initial_ranges(114, 11_820_000) == [(1, 114)]

    def test_file_exactly_at_the_byte_limit_is_a_single_range(self):
        # file_bytes == max_bytes still fits in one call.
        assert initial_ranges(10, MAX_FILE_BYTES) == [(1, 10)]

    def test_single_page_file_is_a_single_range_even_if_oversized(self):
        # total_pages <= 1: there is nothing left to split into.
        assert initial_ranges(1, 20_000_000) == [(1, 1)]

    def test_oversized_file_is_split_into_even_ranges_from_the_average_page_size(self):
        assert initial_ranges(66, 15_190_000) == [(1, 33), (34, 66)]
        assert initial_ranges(40, 15_190_000) == [(1, 20), (21, 40)]

    def test_oversized_file_with_several_full_size_ranges_and_a_short_last_one(self):
        assert initial_ranges(50, 60_000_000) == [
            (1, 9),
            (10, 18),
            (19, 27),
            (28, 36),
            (37, 45),
            (46, 50),
        ]

    def test_custom_max_bytes_is_honored_instead_of_the_default(self):
        assert initial_ranges(10, 30_000_000, max_bytes=10_000_000) == [
            (1, 2),
            (3, 4),
            (5, 6),
            (7, 8),
            (9, 10),
        ]

    def test_ranges_are_consecutive_non_overlapping_and_cover_1_to_total(self):
        ranges = initial_ranges(50, 60_000_000)
        assert ranges[0][0] == 1
        assert ranges[-1][1] == 50
        for (_, end), (next_start, _) in zip(ranges, ranges[1:]):
            assert next_start == end + 1


# ---------------------------------------------------------------------------
# with_status -- prepends the extraction_status block to a response_format's
# schema so every AI_COMPLETE call reports how much of its assigned pages it
# covered, without disturbing the caller's own fields. "required" (and every
# other unsupported keyword) is dropped everywhere via drop_unsupported
# rather than merged, since Cortex json mode rejects a response outright if
# any field it lists as required comes back null -- which a legitimately-
# partial page range triggers constantly.
# ---------------------------------------------------------------------------


class TestWithStatus:
    def test_status_field_is_the_first_property(self):
        response_format = {
            "type": "json",
            "schema": {
                "type": "object",
                "properties": {"name": {"type": "string"}},
            },
        }
        result = with_status(response_format)
        properties = result["schema"]["properties"]
        assert list(properties.keys())[0] == STATUS_FIELD
        # The scalar property is made nullable (see TestNullableScalars).
        assert properties["name"] == {"type": ["string", "null"]}

    def test_output_has_no_required_key_anywhere(self):
        response_format = {
            "type": "json",
            "schema": {
                "type": "object",
                "required": ["name"],
                "properties": {
                    "name": {"type": "string"},
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["sku"],
                            "properties": {"sku": {"type": "string"}},
                        },
                    },
                },
            },
        }
        result = with_status(response_format)
        assert not _contains_required(result)

    def test_status_blocks_own_required_is_also_stripped(self):
        # _STATUS_SCHEMA (built into with_status itself, not user-supplied) has
        # "required" both at its own top level and nested inside
        # remaining_page_ranges.items -- both must be gone too.
        response_format = {
            "type": "json",
            "schema": {"type": "object", "properties": {}},
        }
        status_schema = with_status(response_format)["schema"]["properties"][
            STATUS_FIELD
        ]
        assert "required" not in status_schema
        assert (
            "required"
            not in status_schema["properties"]["remaining_page_ranges"]["items"]
        )

    def test_does_not_mutate_the_input(self):
        response_format = {
            "type": "json",
            "schema": {
                "type": "object",
                "required": ["name"],
                "properties": {"name": {"type": "string"}},
            },
        }
        original = copy.deepcopy(response_format)
        with_status(response_format)
        assert response_format == original

    def test_keeps_type_json(self):
        response_format = {"type": "json", "schema": {"type": "object"}}
        assert with_status(response_format)["type"] == "json"

    def test_other_unsupported_keywords_are_also_stripped(self):
        response_format = {
            "type": "json",
            "schema": {
                "type": "object",
                "additionalProperties": False,
                "properties": {"name": {"type": "string", "pattern": "^[A-Z]"}},
            },
        }
        result = with_status(response_format)
        assert "additionalProperties" not in result["schema"]
        assert "pattern" not in result["schema"]["properties"]["name"]

    def test_scalars_are_nullable_at_every_depth_after_wrapping(self):
        response_format = {
            "type": "json",
            "schema": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "address": {
                        "type": "object",
                        "properties": {"city": {"type": "string"}},
                    },
                },
            },
        }
        properties = with_status(response_format)["schema"]["properties"]
        assert properties["name"]["type"] == ["string", "null"]
        # object-typed properties are untouched, but a scalar nested inside one is also
        # made nullable (nullable_scalars recurses -- see TestNullableScalars).
        assert properties["address"]["type"] == "object"
        assert properties["address"]["properties"]["city"]["type"] == ["string", "null"]
        # The status block's own fields report real types, not nullable ones -- it is
        # self-reported by the model, not a value that could be missing from its pages,
        # and _STATUS_SCHEMA is added after nullable_scalars already ran.
        assert properties[STATUS_FIELD]["properties"]["complete"]["type"] == "boolean"


# ---------------------------------------------------------------------------
# nullable_scalars -- a call covering only some pages can't see a value
# printed elsewhere, and a non-null scalar type then tempts the model into a
# placeholder like "<UNKNOWN>" that json mode rejects outright; make every
# scalar-typed schema nullable AT ANY DEPTH: inside properties, items,
# $defs/definitions/patternProperties values, and anyOf/oneOf/allOf/
# prefixItems lists.
# ---------------------------------------------------------------------------


class TestNullableScalars:
    @pytest.mark.parametrize("json_type", ["string", "integer", "number", "boolean"])
    def test_scalar_types_become_nullable(self, json_type):
        schema = {"type": json_type}
        assert nullable_scalars(schema) == {"type": [json_type, "null"]}

    @pytest.mark.parametrize("json_type", ["object", "array", "null"])
    def test_object_array_and_null_types_are_untouched(self, json_type):
        schema = {"type": json_type}
        assert nullable_scalars(schema) == schema

    def test_already_list_typed_property_is_untouched(self):
        schema = {"type": ["string", "integer"]}
        assert nullable_scalars(schema) == schema

    def test_untyped_schema_is_untouched(self):
        schema = {"description": "no type given"}
        assert nullable_scalars(schema) == schema

    @pytest.mark.parametrize("value", ["hello", 42, True, None, 3.14])
    def test_non_dict_values_pass_through_unchanged(self, value):
        assert nullable_scalars(value) == value

    def test_scalars_nested_under_properties_are_nullable(self):
        schema = {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "address": {
                    "type": "object",
                    "properties": {"city": {"type": "string"}},
                },
            },
        }
        result = nullable_scalars(schema)
        assert result["properties"]["name"]["type"] == ["string", "null"]
        assert result["properties"]["address"]["type"] == "object"
        # Nested arbitrarily deep -- "city" is two properties levels down.
        assert result["properties"]["address"]["properties"]["city"]["type"] == [
            "string",
            "null",
        ]

    def test_scalars_inside_array_items_are_nullable(self):
        schema = {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {"sku": {"type": "string"}},
            },
        }
        result = nullable_scalars(schema)
        assert result["type"] == "array"
        assert result["items"]["properties"]["sku"]["type"] == ["string", "null"]

    @pytest.mark.parametrize("key", ["$defs", "definitions", "patternProperties"])
    def test_scalars_inside_name_maps_are_nullable(self, key):
        schema = {key: {"Foo": {"type": "integer"}}}
        result = nullable_scalars(schema)
        assert result[key]["Foo"]["type"] == ["integer", "null"]

    @pytest.mark.parametrize("key", ["anyOf", "oneOf", "allOf", "prefixItems"])
    def test_scalars_inside_subschema_lists_are_nullable(self, key):
        schema = {key: [{"type": "string"}, {"type": "integer"}]}
        result = nullable_scalars(schema)
        assert result[key] == [
            {"type": ["string", "null"]},
            {"type": ["integer", "null"]},
        ]

    def test_does_not_mutate_the_input(self):
        schema = {
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "items": {"type": "integer"},
            "$defs": {"Foo": {"type": "string"}},
            "anyOf": [{"type": "boolean"}],
        }
        original = copy.deepcopy(schema)
        nullable_scalars(schema)
        assert schema == original


# ---------------------------------------------------------------------------
# build_predicate -- three scopes depending on where this file's pages sit in
# the original document: the whole document, the first part of a split, or a
# later part (which carries one page of context and needs the file-page ->
# document-page mapping explained to the model).
# ---------------------------------------------------------------------------


class TestBuildPredicate:
    def test_base_prompt_comes_first(self):
        predicate = build_predicate("MY BASE PROMPT", 100, 1, 1, 1)
        assert predicate.startswith("MY BASE PROMPT\n\n")

    def test_whole_document_scope(self):
        predicate = build_predicate("Extract fields.", 4000, 1, 10, 10)
        assert "This file is the whole document: document pages 1 to 10." in predicate

    def test_first_part_scope(self):
        predicate = build_predicate("Extract fields.", 4000, 1, 5, 20)
        assert "This file is the first 5 pages of a longer document." in predicate
        assert (
            "Page numbers in extraction_status are 1-based positions within THIS file"
            in predicate
        )

    def test_later_part_scope_explains_the_context_page(self):
        predicate = build_predicate("Extract fields.", 4000, 30, 48, 66)
        # end - start + 2 = 20: the context page plus the 19 real pages of the part.
        assert (
            "This file is 20 consecutive pages from the middle of a longer document."
            in predicate
        )
        assert "The FIRST page of this file is context only" in predicate
        assert "Extract from the SECOND page of this file onward." in predicate
        assert (
            "Page numbers in extraction_status are 1-based positions within THIS file "
            "(the context page is position 1)" in predicate
        )

    def test_target_token_count_appears(self):
        predicate = build_predicate("Extract fields.", 1234, 1, 10, 10)
        assert "is 1234 tokens or less" in predicate
        assert "under 1234 output tokens" in predicate

    def test_includes_the_null_sentence_for_missing_values(self):
        predicate = build_predicate("Extract fields.", 4000, 1, 10, 10)
        assert (
            "If a requested single value is not shown on the pages you must extract, "
            "return null for it" in predicate
        )
        assert "never write a placeholder such as '<UNKNOWN>' or 'N/A'" in predicate

    def test_plan_target_defaults_to_target(self):
        predicate = build_predicate("Extract fields.", 1234, 1, 10, 10)
        assert "expected to need under 1234 output tokens" in predicate

    def test_plan_target_sizes_only_the_remaining_page_ranges_sentence(self):
        # target still bounds THIS response's own stop/extract decision;
        # plan_target only sizes the ranges planned for what's left over.
        predicate = build_predicate(
            "Extract fields.", 10000, 1, 10, 10, plan_target=40000
        )
        assert "is 10000 tokens or less" in predicate
        assert "under 10000 output tokens, never stopping in the middle" in predicate
        assert "expected to need under 40000 output tokens" in predicate
        assert "40000 tokens or less" not in predicate


# ---------------------------------------------------------------------------
# unwrap_envelope -- a model can nest its whole answer under one
# undeclared key (e.g. {"input": {...}}); return that inner object instead,
# but only when it names more declared top-level fields than the top level
# already does. A declared key is never itself treated as an envelope.
# ---------------------------------------------------------------------------


class TestUnwrapEnvelope:
    SCHEMA = {"properties": {"a": {"type": "string"}, "b": {"type": "string"}}}

    def test_single_envelope_naming_every_declared_field_is_unwrapped(self):
        value = {"input": {"a": 1, "b": 2}}
        assert unwrap_envelope(value, self.SCHEMA) == {"a": 1, "b": 2}

    def test_a_dollar_parameter_name_envelope_is_unwrapped(self):
        # Seen live (claude-opus-5): an envelope named after the SQL
        # bind parameter rather than anything schema-related.
        value = {"$PARAMETER_NAME": {"a": 1, "b": 2}}
        assert unwrap_envelope(value, self.SCHEMA) == {"a": 1, "b": 2}

    def test_already_populated_top_level_is_left_unchanged(self):
        # The top level already names both declared fields, so the "input"
        # envelope (which only ties that count, not beats it) is not preferred.
        value = {"a": 1, "b": 2, "input": {"a": 9, "b": 9}}
        assert unwrap_envelope(value, self.SCHEMA) == value

    def test_a_declared_key_is_never_treated_as_an_envelope(self):
        schema = {
            "properties": {"address": {"type": "object"}, "name": {"type": "string"}}
        }
        value = {"address": {"street": "x"}}
        assert unwrap_envelope(value, schema) == value

    def test_the_candidate_naming_the_most_declared_fields_wins(self):
        value = {"wrapper1": {"a": 1}, "wrapper2": {"a": 1, "b": 2}}
        assert unwrap_envelope(value, self.SCHEMA) == {"a": 1, "b": 2}

    def test_schema_without_properties_leaves_value_unchanged(self):
        value = {"input": {"a": 1}}
        assert unwrap_envelope(value, {}) == value
        assert unwrap_envelope(value, {"properties": {}}) == value

    @pytest.mark.parametrize("value", [None, "not a dict", 42])
    def test_non_dict_value_passes_through_unchanged(self, value):
        assert unwrap_envelope(value, self.SCHEMA) == value


# ---------------------------------------------------------------------------
# _to_document_page -- the model is always asked to report extraction_status
# page numbers as 1-based positions WITHIN THE PART FILE, never as document
# pages; this converts a reported file position back to the document page it
# corresponds to (parts after the first carry one page of context at
# position 1, so their positions are offset from document pages by start-2).
# ---------------------------------------------------------------------------


class TestToDocumentPage:
    def test_first_part_file_position_equals_document_page(self):
        # start=1: the offset is 0, so a file position IS the document page.
        assert _to_document_page(5, 1, 10) == 5
        assert _to_document_page(10, 1, 10) == 10

    def test_file_position_maps_to_document_page_for_a_later_part(self):
        # start=30, end=48: file position 1 is the context page (document page
        # 29), file position 20 is the last page of the part (document page 48).
        assert _to_document_page(1, 30, 48) == 29
        assert _to_document_page(20, 30, 48) == 48

    def test_non_int_or_bool_or_none_is_none(self):
        assert _to_document_page(None, 1, 10) is None
        assert _to_document_page("5", 1, 10) is None
        assert _to_document_page(5.0, 1, 10) is None
        # bool is an int subclass but is never a legitimate page number.
        assert _to_document_page(True, 1, 10) is None

    def test_out_of_range_is_none(self):
        assert _to_document_page(15, 1, 10) is None
        assert _to_document_page(100, 30, 48) is None
        # Maps to document page 28, which is before the context page (29):
        # not a valid file page for this part either.
        assert _to_document_page(0, 30, 48) is None
        # One past the last file position (20) for this part: maps to document
        # page 49, past end (48).
        assert _to_document_page(21, 30, 48) is None


# ---------------------------------------------------------------------------
# document_estimate -- scales one call's own estimated_total_output_tokens
# (for the pages it alone covers) up to what the whole document would need,
# by the ratio of total pages to this call's page count. None without a
# usable positive int estimate to scale.
# ---------------------------------------------------------------------------


class TestDocumentEstimate:
    def test_scales_the_estimate_to_the_whole_document(self):
        value = {STATUS_FIELD: {"estimated_total_output_tokens": 48000}}
        assert document_estimate(value, 30, 34, 114) == 1_094_400

    def test_truncates_towards_zero_rather_than_rounding(self):
        # 100 * 7 / 3 == 233.33...; int() truncates rather than rounding to 233.
        value = {STATUS_FIELD: {"estimated_total_output_tokens": 100}}
        assert document_estimate(value, 1, 3, 7) == 233

    def test_a_degenerate_range_falls_back_to_a_denominator_of_1(self):
        # end < start: end - start + 1 is 0 or negative, so the denominator floors at 1
        # rather than dividing by zero or inflating the estimate with a negative one.
        value = {STATUS_FIELD: {"estimated_total_output_tokens": 100}}
        assert document_estimate(value, 5, 3, 10) == 1000

    @pytest.mark.parametrize(
        "value",
        [
            None,
            "not a dict",
            42,
            {},
            {STATUS_FIELD: "not a dict"},
            {STATUS_FIELD: {}},
        ],
    )
    def test_missing_or_non_dict_value_or_status_is_none(self, value):
        assert document_estimate(value, 1, 10, 100) is None

    @pytest.mark.parametrize("estimate", [None, "48000", True, 0, -5])
    def test_missing_non_int_bool_or_non_positive_estimate_is_none(self, estimate):
        value = {STATUS_FIELD: {"estimated_total_output_tokens": estimate}}
        assert document_estimate(value, 1, 10, 100) is None


# ---------------------------------------------------------------------------
# shift_page_fields -- rows in a part file report page numbers as positions
# WITHIN THAT FILE; shift a page field to document pages only when every row's
# value for it is an int that actually lies within the part's own content
# pages (2..file_pages; position 1 is the context page carried by parts after
# the first) -- a page field that instead holds page numbers printed on the
# document (which run on past the part) is left untouched. start=1 (and
# start=2, which also has a zero offset) is always a no-op.
# ---------------------------------------------------------------------------


class TestShiftPageFields:
    def test_values_within_the_part_are_shifted_by_the_offset(self):
        # start=30, end=34: offset = file_page_offset(30) = 28.
        value = {"rows": [{"source_page": 2}, {"source_page": 4}]}
        result = shift_page_fields(value, 30, 34)
        assert result == {"rows": [{"source_page": 30}, {"source_page": 32}]}

    def test_values_outside_the_parts_content_pages_are_left_unchanged(self):
        # start=35, end=66: file_pages = 66 - 35 + 2 = 33; these look like page
        # numbers printed on the document (34..40), which exceed file_pages, so
        # the field is left alone rather than shifted into nonsense.
        value = {"rows": [{"printed_page": p} for p in range(34, 41)]}
        result = shift_page_fields(value, 35, 66)
        assert result == value

    def test_a_context_page_position_anywhere_blocks_the_shift_for_every_row(self):
        # One row reporting position 1 (the context page) means this key isn't a
        # content page field after all, so no row for this key is shifted.
        value = {"rows": [{"page": 1}, {"page": 3}]}
        result = shift_page_fields(value, 30, 34)
        assert result == value

    def test_mixed_types_in_a_page_field_block_the_shift(self):
        value = {"rows": [{"page": 2}, {"page": "3"}]}
        result = shift_page_fields(value, 30, 34)
        assert result == value

    def test_a_bool_value_in_a_page_field_blocks_the_shift(self):
        value = {"rows": [{"page": 2}, {"page": True}]}
        result = shift_page_fields(value, 30, 34)
        assert result == value

    def test_none_values_are_left_as_none_and_do_not_block_other_rows(self):
        value = {"rows": [{"page": 2}, {"page": None}]}
        result = shift_page_fields(value, 30, 34)
        assert result == {"rows": [{"page": 30}, {"page": None}]}

    @pytest.mark.parametrize(
        "key", ["page", "page_number", "pdf_page", "page_index", "source_page", "PAGE"]
    )
    def test_recognized_page_field_names_are_shifted(self, key):
        value = {"rows": [{key: 2}]}
        assert shift_page_fields(value, 30, 34) == {"rows": [{key: 30}]}

    def test_an_unrecognized_field_name_is_never_shifted(self):
        value = {"rows": [{"count": 2}]}
        assert shift_page_fields(value, 30, 34) == value

    def test_nested_arrays_inside_objects_are_shifted(self):
        value = {
            "funds": [
                {
                    "name": "A",
                    "holdings": [{"page": 2}, {"page": 3}],
                }
            ]
        }
        result = shift_page_fields(value, 30, 34)
        assert result == {
            "funds": [
                {
                    "name": "A",
                    "holdings": [{"page": 30}, {"page": 31}],
                }
            ]
        }

    def test_the_status_block_is_never_shifted(self):
        value = {
            STATUS_FIELD: {"last_pdf_page_extracted": 2},
            "rows": [{"page": 2}],
        }
        result = shift_page_fields(value, 30, 34)
        assert result[STATUS_FIELD] == {"last_pdf_page_extracted": 2}
        assert result["rows"] == [{"page": 30}]

    def test_start_1_is_a_no_op(self):
        value = {"rows": [{"page": 2}]}
        assert shift_page_fields(value, 1, 10) == value

    def test_start_2_is_also_a_no_op(self):
        # file_page_offset(2) == 0 too: position 1 already IS document page 1.
        value = {"rows": [{"page": 2}]}
        assert shift_page_fields(value, 2, 10) == value

    def test_does_not_mutate_the_input(self):
        value = {"rows": [{"page": 2}, {"page": 4}]}
        original = copy.deepcopy(value)
        shift_page_fields(value, 30, 34)
        assert value == original


# ---------------------------------------------------------------------------
# next_ranges -- decides, from one AI_COMPLETE call's result, whether to keep
# its value and what (if anything) still needs to be re-extracted.
# ---------------------------------------------------------------------------


class TestNextRanges:
    def test_truncation_error_on_a_multi_page_call_halves_the_range(self):
        # Only truncation is worth retrying with smaller pages.
        keep, ranges = next_ranges(1, 10, {"name": "x"}, "unexpected end of JSON input")
        assert keep is False
        assert ranges == [(1, 5), (6, 10)]

    def test_non_truncation_error_on_a_multi_page_call_gives_up_with_no_ranges(self):
        # A non-splittable error (e.g. a content filter or rate limit) won't be
        # fixed by splitting the pages, so it isn't retried at all.
        keep, ranges = next_ranges(1, 10, {"name": "x"}, "boom")
        assert keep is False
        assert ranges == []

    def test_malformed_request_error_on_a_multi_page_call_halves_the_range(self):
        # Cortex's own "file too large" rejection is also worth retrying smaller.
        keep, ranges = next_ranges(1, 10, {"name": "x"}, "malformed request")
        assert keep is False
        assert ranges == [(1, 5), (6, 10)]

    def test_empty_answer_error_on_a_multi_page_call_halves_the_range(self):
        # run() converts an error-free empty object into EMPTY_ANSWER_ERROR;
        # fewer pages per call is also worth retrying for that.
        keep, ranges = next_ranges(1, 40, None, EMPTY_ANSWER_ERROR)
        assert keep is False
        assert ranges == [(1, 20), (21, 40)]

    def test_empty_answer_error_on_a_single_page_call_gives_up_with_no_ranges(self):
        keep, ranges = next_ranges(5, 5, None, EMPTY_ANSWER_ERROR)
        assert keep is False
        assert ranges == []

    def test_truncation_error_on_a_single_page_call_gives_up_with_no_ranges(self):
        keep, ranges = next_ranges(5, 5, None, "unexpected end of JSON input")
        assert keep is False
        assert ranges == []

    def test_non_truncation_error_on_a_single_page_call_gives_up_with_no_ranges(self):
        keep, ranges = next_ranges(5, 5, None, "boom")
        assert keep is False
        assert ranges == []

    def test_malformed_request_error_on_a_single_page_call_gives_up_with_no_ranges(
        self,
    ):
        # Nothing left to split: one page is already the smallest unit.
        keep, ranges = next_ranges(5, 5, None, "malformed request")
        assert keep is False
        assert ranges == []

    def test_non_dict_value_without_an_error_is_treated_like_a_truncation(self):
        keep, ranges = next_ranges(1, 10, "not a dict", None)
        assert keep is False
        assert ranges == [(1, 5), (6, 10)]

    def test_missing_status_block_on_a_multi_page_call_halves_the_range(self):
        # The model may have stopped early without saying so: don't trust the answer.
        keep, ranges = next_ranges(1, 10, {"name": "x"}, None)
        assert keep is False
        assert ranges == [(1, 5), (6, 10)]

    def test_non_dict_status_on_a_multi_page_call_halves_the_range(self):
        keep, ranges = next_ranges(1, 10, {STATUS_FIELD: "not a dict"}, None)
        assert keep is False
        assert ranges == [(1, 5), (6, 10)]

    def test_missing_status_halves_an_odd_page_count_without_losing_a_page(self):
        keep, ranges = next_ranges(3, 9, {"name": "x"}, None)
        assert keep is False
        assert ranges == [(3, 6), (7, 9)]

    def test_missing_status_on_a_two_page_call_gives_one_page_halves(self):
        keep, ranges = next_ranges(4, 5, {"name": "x"}, None)
        assert keep is False
        assert ranges == [(4, 4), (5, 5)]

    def test_missing_or_non_dict_status_on_a_single_page_call_is_kept(self):
        assert next_ranges(5, 5, {"name": "x"}, None) == (True, [])
        assert next_ranges(5, 5, {STATUS_FIELD: "not a dict"}, None) == (True, [])

    def test_complete_true_or_absent_in_a_status_object_is_treated_as_complete(self):
        assert next_ranges(1, 10, {STATUS_FIELD: {"complete": True}}, None) == (
            True,
            [],
        )
        assert next_ranges(1, 10, {STATUS_FIELD: {}}, None) == (True, [])

    def test_complete_false_with_contiguous_planned_ranges_is_kept(self):
        value = {
            STATUS_FIELD: {
                "complete": False,
                "last_pdf_page_extracted": 29,
                "remaining_page_ranges": [
                    {"start_pdf_page": 30, "end_pdf_page": 48},
                    {"start_pdf_page": 49, "end_pdf_page": 66},
                ],
            }
        }
        keep, ranges = next_ranges(1, 66, value, None)
        assert keep is True
        assert ranges == [(30, 48), (49, 66)]

    def test_a_gap_in_the_planned_ranges_is_filled_by_cover(self):
        value = {
            STATUS_FIELD: {
                "complete": False,
                "last_pdf_page_extracted": 29,
                "remaining_page_ranges": [{"start_pdf_page": 35, "end_pdf_page": 40}],
            }
        }
        keep, ranges = next_ranges(1, 66, value, None)
        assert keep is True
        assert ranges == [(30, 34), (35, 40), (41, 66)]

    def test_an_overlap_in_the_planned_ranges_is_trimmed_by_cover(self):
        value = {
            STATUS_FIELD: {
                "complete": False,
                "last_pdf_page_extracted": 29,
                "remaining_page_ranges": [
                    {"start_pdf_page": 30, "end_pdf_page": 50},
                    {"start_pdf_page": 45, "end_pdf_page": 66},
                ],
            }
        }
        keep, ranges = next_ranges(1, 66, value, None)
        assert keep is True
        assert ranges == [(30, 50), (51, 66)]

    def test_an_out_of_range_planned_entry_is_dropped_and_cover_fills_the_gap(self):
        # end_pdf_page=80 doesn't map to any document page in a 66-page document,
        # so the whole entry is discarded rather than clipped -- _cover() still
        # has to fill in everything from last+1 to end on its own.
        value = {
            STATUS_FIELD: {
                "complete": False,
                "last_pdf_page_extracted": 29,
                "remaining_page_ranges": [{"start_pdf_page": 20, "end_pdf_page": 80}],
            }
        }
        keep, ranges = next_ranges(1, 66, value, None)
        assert keep is True
        assert ranges == [(30, 66)]

    def test_last_page_given_in_file_pages_is_mapped_before_computing_remaining(self):
        # start=30, end=48: last_pdf_page_extracted=10 is a FILE page, which maps
        # to document page 38 -- so the remaining range starts at document page 39,
        # not file page 11.
        value = {STATUS_FIELD: {"complete": False, "last_pdf_page_extracted": 10}}
        keep, ranges = next_ranges(30, 48, value, None)
        assert keep is True
        assert ranges == [(39, 48)]

    def test_no_progress_on_a_multi_page_call_halves_the_range(self):
        # last_pdf_page_extracted=1 maps to document page 29 here, the context
        # page preceding this part (start-1) -- not real progress on pages 30-48.
        value = {STATUS_FIELD: {"complete": False, "last_pdf_page_extracted": 1}}
        keep, ranges = next_ranges(30, 48, value, None)
        assert keep is False
        assert ranges == [(30, 39), (40, 48)]

    def test_no_progress_on_a_single_page_call_gives_up_with_no_ranges(self):
        value = {STATUS_FIELD: {"complete": False, "last_pdf_page_extracted": 1}}
        keep, ranges = next_ranges(30, 30, value, None)
        assert keep is True
        assert ranges == []

    def test_missing_last_pdf_page_extracted_also_counts_as_no_progress(self):
        value = {STATUS_FIELD: {"complete": False}}
        keep, ranges = next_ranges(1, 10, value, None)
        assert keep is False
        assert ranges == [(1, 5), (6, 10)]

    def test_last_at_or_beyond_end_is_treated_as_complete(self):
        value = {STATUS_FIELD: {"complete": False, "last_pdf_page_extracted": 10}}
        keep, ranges = next_ranges(1, 10, value, None)
        assert keep is True
        assert ranges == []

    # -- target: without it, behavior is unchanged from above; with it, the covered
    # ranges are passed through coalesce_ranges using each planned range's own
    # estimated_output_tokens (see TestCoalesceRanges).

    REMAINING_WITH_ESTIMATES = {
        STATUS_FIELD: {
            "complete": False,
            "last_pdf_page_extracted": 9,
            "remaining_page_ranges": [
                {
                    "start_pdf_page": 10,
                    "end_pdf_page": 20,
                    "estimated_output_tokens": 30000,
                },
                {
                    "start_pdf_page": 21,
                    "end_pdf_page": 30,
                    "estimated_output_tokens": 30000,
                },
                {
                    "start_pdf_page": 31,
                    "end_pdf_page": 40,
                    "estimated_output_tokens": 30000,
                },
                {"start_pdf_page": 41, "end_pdf_page": 50},
            ],
        }
    }

    def test_omitting_target_keeps_the_covered_ranges_uncoalesced(self):
        keep, ranges = next_ranges(1, 50, self.REMAINING_WITH_ESTIMATES, None)
        assert keep is True
        assert ranges == [(10, 20), (21, 30), (31, 40), (41, 50)]

    def test_target_coalesces_the_covered_ranges(self):
        keep, ranges = next_ranges(
            1, 50, self.REMAINING_WITH_ESTIMATES, None, target=60000
        )
        assert keep is True
        # The first three ranges' estimates (30000 each) merge two at a time under the
        # 60000 target, and the last range (no estimate) never merges.
        assert ranges == [(10, 30), (31, 40), (41, 50)]


# ---------------------------------------------------------------------------
# coalesce_ranges -- a plan finer than the output-token target asks for is
# merged while adjacent ranges' summed estimates stay within target; a range
# with no estimate is never merged into, or merged from.
# ---------------------------------------------------------------------------


class TestCoalesceRanges:
    def test_consecutive_ranges_merge_while_under_target_and_a_none_estimate_stops_it(
        self,
    ):
        ranges = [
            (10, 20, 30000),
            (21, 30, 30000),
            (31, 40, 30000),
            (41, 50, None),
        ]
        assert coalesce_ranges(ranges, 60000) == [(10, 30), (31, 40), (41, 50)]

    def test_a_none_estimate_range_is_never_merged_into_by_the_next_range(self):
        # The None-estimate range is appended on its own; the following range's own
        # estimate can't be added onto a None running total either.
        ranges = [(1, 5, None), (6, 10, 1000)]
        assert coalesce_ranges(ranges, 100_000) == [(1, 5), (6, 10)]

    def test_a_single_range_passes_through_unchanged(self):
        assert coalesce_ranges([(1, 5, 1000)], 100_000) == [(1, 5)]

    def test_empty_ranges_gives_an_empty_list(self):
        assert coalesce_ranges([], 100_000) == []

    def test_merging_stops_once_the_target_would_be_exceeded(self):
        # 20000 + 20000 = 40000 <= 40000 merges; the third range's own
        # 20000 would push the running total to 60000, over target, so it starts a
        # new group instead.
        ranges = [(1, 10, 20000), (11, 20, 20000), (21, 30, 20000)]
        assert coalesce_ranges(ranges, 40000) == [(1, 20), (21, 30)]


# ---------------------------------------------------------------------------
# call_cap -- the per-file AI_COMPLETE call budget: twice the number of
# target-sized ranges the estimated answer needs, plus slack, never below
# the starting floor nor above HARD_MAX_CALLS_PER_FILE.
# ---------------------------------------------------------------------------


class TestCallCap:
    @pytest.mark.parametrize(
        "floor, estimated_total, range_target, expected",
        [
            (12, 450_000, 40_000, 28),
            (12, 5_000, 40_000, 12),
            (12, 840_000, 40_000, 46),
        ],
    )
    def test_worked_examples(self, floor, estimated_total, range_target, expected):
        assert call_cap(floor, estimated_total, range_target) == expected

    def test_a_huge_estimate_is_capped_at_the_hard_max(self):
        assert call_cap(12, 100_000_000, 1000) == HARD_MAX_CALLS_PER_FILE

    @pytest.mark.parametrize("estimated_total", [None, 0])
    def test_a_falsy_estimated_total_returns_the_floor_unchanged(self, estimated_total):
        assert call_cap(12, estimated_total, 40_000) == 12


# ---------------------------------------------------------------------------
# _cover -- makes a model's self-reported remaining_page_ranges consecutive,
# non-overlapping and exactly covering [first, last], no matter how broken
# the input is.
# ---------------------------------------------------------------------------


class TestCover:
    def test_fills_a_gap(self):
        assert _cover([(35, 40)], 30, 66) == [(30, 34), (35, 40), (41, 66)]

    def test_trims_an_overlap(self):
        assert _cover([(30, 50), (45, 66)], 30, 66) == [(30, 50), (51, 66)]

    def test_empty_ranges_covers_the_whole_span(self):
        assert _cover([], 30, 48) == [(30, 48)]


# ---------------------------------------------------------------------------
# continues_across_split -- true when the last row of one range and the first
# row of the next are really one row cut in half by the page split: they must
# not conflict on any shared non-absent value, share at least one equal
# non-absent scalar (an identifying field), and each must fill in something
# the other lacks. Two complete rows -- even an exact duplicate -- are never
# joined, so a row printed twice on purpose stays twice.
# ---------------------------------------------------------------------------


class TestContinuesAcrossSplit:
    def test_a_fund_split_mid_row_continues(self):
        last = {"fund_name": "A", "holdings": [1], "net_assets": None}
        first = {"fund_name": "A", "holdings": [], "net_assets": 5}
        assert continues_across_split(last, first) is True

    def test_identical_complete_rows_do_not_continue(self):
        # Nothing is absent on either side, so there is nothing left to fill in --
        # a row printed twice (e.g. a repeated header row) stays two rows.
        last = {"fund_name": "A", "net_assets": 5}
        first = {"fund_name": "A", "net_assets": 5}
        assert continues_across_split(last, first) is False

    def test_a_conflicting_value_on_a_shared_key_does_not_continue(self):
        # Same subject, but a different (non-absent) value for the same field --
        # e.g. two different timepoints for fund A -- is a real conflict.
        last = {"fund_name": "A", "date": "Jan"}
        first = {"fund_name": "A", "date": "Feb"}
        assert continues_across_split(last, first) is False

    def test_no_shared_key_does_not_continue(self):
        # Nothing to agree on, so there is no identifying scalar to join them by.
        last = {"fund_name": "A"}
        first = {"net_assets": 5}
        assert continues_across_split(last, first) is False

    @pytest.mark.parametrize(
        "last, first",
        [
            (None, {"a": 1}),
            ({"a": 1}, None),
            ("not a dict", {"a": 1}),
            ({"a": 1}, [1, 2]),
        ],
    )
    def test_non_dict_arguments_do_not_continue(self, last, first):
        assert continues_across_split(last, first) is False

    def test_shared_list_valued_keys_are_ignored_for_conflicts_and_for_filling(self):
        # holdings differ on both sides, but lists are never compared or counted
        # towards fills_last/fills_first -- only net_assets (scalar) does that.
        last = {"fund_name": "A", "holdings": [1], "net_assets": None}
        first = {"fund_name": "A", "holdings": [2, 3], "net_assets": 5}
        assert continues_across_split(last, first) is True

    def test_nested_dicts_are_compared_recursively_rather_than_by_equality(self):
        # The nested "address" dicts don't match by == (one is missing "zip"), but
        # they themselves continue across the split (zip fills in what last lacks),
        # so they are not treated as a conflict; net_assets supplies the fill that
        # makes the outer rows continue.
        last = {"fund_name": "A", "net_assets": None, "address": {"city": "NYC"}}
        first = {
            "fund_name": "A",
            "net_assets": 5,
            "address": {"city": "NYC", "zip": "10001"},
        }
        assert continues_across_split(last, first) is True

    def test_conflicting_nested_dicts_do_not_continue(self):
        last = {"fund_name": "A", "net_assets": None, "address": {"city": "NYC"}}
        first = {"fund_name": "A", "net_assets": 5, "address": {"city": "Boston"}}
        assert continues_across_split(last, first) is False


# ---------------------------------------------------------------------------
# merge_values -- folds the per-range answers (given in page order) back into
# one object: arrays concatenate (joining the item a split cut in two, via
# continues_across_split), nested objects merge field by field, scalars
# keep the first present value, and extraction_status never leaks into the
# final answer.
# ---------------------------------------------------------------------------


class TestMergeValues:
    def test_arrays_concatenate_in_order(self):
        parts = [{"items": [1, 2]}, {"items": [3, 4]}]
        assert merge_values(parts) == {"items": [1, 2, 3, 4]}

    def test_a_row_cut_by_the_split_is_spliced_instead_of_duplicated(self):
        # Fund A's holdings end one range and its net_assets (and more holdings)
        # open the next -- continues_across_split recognizes the boundary pair and
        # merge_values splices them into one row instead of appending a duplicate.
        parts = [
            {"funds": [{"fund_name": "A", "holdings": [1], "net_assets": None}]},
            {
                "funds": [
                    {"fund_name": "A", "holdings": [2], "net_assets": 5},
                    {"fund_name": "B", "holdings": [3]},
                ]
            },
        ]
        assert merge_values(parts) == {
            "funds": [
                {"fund_name": "A", "holdings": [1, 2], "net_assets": 5},
                {"fund_name": "B", "holdings": [3]},
            ]
        }

    def test_dict_rows_that_do_not_continue_concatenate_normally(self):
        # Different ids on a shared key is a conflict, not a continuation, so these
        # two complete rows stay two rows -- a normal (non-split) row table.
        parts = [
            {"rows": [{"id": 1, "val": "x"}]},
            {"rows": [{"id": 2, "val": "y"}]},
        ]
        assert merge_values(parts) == {
            "rows": [{"id": 1, "val": "x"}, {"id": 2, "val": "y"}]
        }

    def test_nested_objects_merge_field_by_field(self):
        parts = [
            {"address": {"city": "Springfield"}},
            {"address": {"zip": "00000"}},
        ]
        assert merge_values(parts) == {
            "address": {"city": "Springfield", "zip": "00000"}
        }

    def test_scalars_keep_the_first_present_value(self):
        assert merge_values([{"name": "first"}, {"name": "second"}]) == {
            "name": "first"
        }

    @pytest.mark.parametrize("absent_value", [None, "", [], {}])
    def test_none_empty_string_and_empty_collections_are_treated_as_absent(
        self, absent_value
    ):
        parts = [{"name": absent_value}, {"name": "Alice"}]
        assert merge_values(parts) == {"name": "Alice"}

    def test_extraction_status_is_stripped_from_every_part(self):
        parts = [
            {STATUS_FIELD: {"complete": False}, "name": "Alice"},
            {STATUS_FIELD: {"complete": True}, "age": 30},
        ]
        result = merge_values(parts)
        assert STATUS_FIELD not in result
        assert result == {"name": "Alice", "age": 30}

    def test_empty_parts_list_gives_an_empty_dict(self):
        assert merge_values([]) == {}


# ---------------------------------------------------------------------------
# qualify_listed_name -- LIST returns "<stage name>/<relative path>" with no
# database/schema qualification; rebuild a reference against the caller's own
# (possibly fully-qualified) stage path so it resolves regardless of the
# session's current schema.
# ---------------------------------------------------------------------------


class TestQualifyListedName:
    def test_fully_qualified_stage_with_a_file_path(self):
        assert (
            qualify_listed_name("@DB.SCH.STAGE/dir/x.pdf", "stage/dir/x.pdf")
            == "@DB.SCH.STAGE/dir/x.pdf"
        )

    def test_fully_qualified_stage_with_a_folder_path(self):
        assert (
            qualify_listed_name("@DB.SCH.STAGE/one/", "stage/one/a.pdf")
            == "@DB.SCH.STAGE/one/a.pdf"
        )

    def test_unqualified_stage(self):
        assert qualify_listed_name("@mystage", "mystage/a.pdf") == "@mystage/a.pdf"


# ---------------------------------------------------------------------------
# schema_instruction -- the fixed instruction text sent to claude-sonnet-5-5
# (which has no response_format/json mode): asks for exactly one JSON object,
# with extraction_status filled first, followed by the schema itself as
# compact (no extra whitespace) JSON.
# ---------------------------------------------------------------------------


class TestSchemaInstruction:
    def test_instruction_text_covers_the_required_points(self):
        text = schema_instruction({"type": "object", "properties": {}})
        assert "ONLY one JSON object" in text
        assert "no prose before or after it and no markdown code fences" in text
        assert "Fill extraction_status first" in text

    def test_schema_round_trips_through_the_tail_after_the_json_schema_tag(self):
        schema = {
            "type": "object",
            "properties": {
                "name": {"type": ["string", "null"]},
                "items": {"type": "array", "items": {"type": "integer"}},
            },
        }
        text = schema_instruction(schema)
        _, tail = text.split("JSON Schema:\n")
        assert json.loads(tail) == schema

    def test_schema_tail_is_compact_json_with_no_extra_whitespace(self):
        schema = {"a": 1, "b": [1, 2]}
        text = schema_instruction(schema)
        _, tail = text.split("JSON Schema:\n")
        assert tail == json.dumps(schema, separators=(",", ":"))
        assert ", " not in tail
        assert ": " not in tail

    def test_json_schema_tag_appears_exactly_once_at_the_end(self):
        text = schema_instruction({"a": 1})
        assert text.endswith(json.dumps({"a": 1}, separators=(",", ":")))
        assert text.count("JSON Schema:\n") == 1


# ---------------------------------------------------------------------------
# parse_text_answer -- claude-sonnet-5-5 has no response_format/json mode, so
# its answer is free text that must still contain exactly one JSON object;
# pull that object out (through markdown fences and surrounding prose) and
# report a cut-off object with the same truncation signature run() already
# knows how to split on.
# ---------------------------------------------------------------------------


class TestParseTextAnswer:
    def test_a_dict_passes_through_unchanged(self):
        value = {"name": "Alice"}
        assert parse_text_answer(value) == (value, None)

    @pytest.mark.parametrize("value", [42, None, [1, 2], True, 3.14])
    def test_non_str_non_dict_is_rejected(self, value):
        assert parse_text_answer(value) == (None, "answer was not text")

    def test_a_plain_json_object_is_parsed(self):
        assert parse_text_answer('{"name": "Alice"}') == ({"name": "Alice"}, None)

    def test_a_json_fenced_block_with_a_trailing_fence_is_parsed(self):
        text = '```json\n{"name": "Alice"}\n```'
        assert parse_text_answer(text) == ({"name": "Alice"}, None)

    def test_a_json_fenced_block_without_a_trailing_fence_is_parsed(self):
        text = '```json\n{"name": "Alice"}'
        assert parse_text_answer(text) == ({"name": "Alice"}, None)

    def test_prose_before_and_after_the_object_is_ignored(self):
        text = (
            'Sure, here is the answer:\n{"name": "Alice"}\n'
            "Let me know if you need more."
        )
        assert parse_text_answer(text) == ({"name": "Alice"}, None)

    def test_an_object_found_inside_a_larger_sentence_is_still_parsed(self):
        text = 'the fields are {"a": 1} and that is all'
        assert parse_text_answer(text) == ({"a": 1}, None)

    def test_no_opening_brace_is_rejected(self):
        assert parse_text_answer("no JSON here") == (
            None,
            "answer contained no JSON object",
        )

    def test_a_top_level_array_with_no_object_is_rejected(self):
        assert parse_text_answer("[1, 2, 3]") == (
            None,
            "answer contained no JSON object",
        )

    def test_json_cut_off_before_closing_is_reported_as_truncated(self):
        value, error = parse_text_answer('{"a": 1, "b": [1,2')
        assert value is None
        assert error == TRUNCATED_TEXT_ERROR

    def test_the_truncation_signature_is_recognized_as_splittable(self):
        assert is_splittable_error(TRUNCATED_TEXT_ERROR) is True

    def test_malformed_json_in_the_middle_is_reported_distinctly_from_truncation(self):
        value, error = parse_text_answer('{"a": 1 "b": 2}')
        assert value is None
        assert error.startswith("answer was not valid JSON")

    def test_a_raw_control_character_inside_a_string_is_accepted(self):
        assert parse_text_answer('{"name": "Alice\tSmith"}') == (
            {"name": "Alice\tSmith"},
            None,
        )

    def test_an_unparseable_answer_is_splittable(self):
        _, error = parse_text_answer('{"a": 1 "b": 2}')
        assert is_splittable_error(error) is True

    def test_an_answer_with_no_json_object_is_splittable(self):
        _, error = parse_text_answer("no JSON here")
        assert is_splittable_error(error) is True


# ---------------------------------------------------------------------------
# salvage_status / ranges_from_salvaged_status -- a failed text answer still
# opens with its extraction_status, whose plan re-splits the call's pages
# instead of blind halves.
# ---------------------------------------------------------------------------

_CUT_OFF_ANSWER = (
    '{"extraction_status":{"complete":false,"estimated_total_output_tokens":300000,'
    '"last_pdf_page_extracted":10,"remaining_page_ranges":['
    '{"start_pdf_page":11,"end_pdf_page":30,"estimated_output_tokens":120000},'
    '{"start_pdf_page":31,"end_pdf_page":60,"estimated_output_tokens":120000}]},'
    '"rows":[{"a":1},{"a":'
)


class TestSalvageStatus:
    def test_the_status_of_a_cut_off_answer_is_recovered(self):
        status = salvage_status(_CUT_OFF_ANSWER)
        assert status["last_pdf_page_extracted"] == 10
        assert len(status["remaining_page_ranges"]) == 2

    def test_an_answer_cut_off_inside_its_status_gives_none(self):
        assert salvage_status('{"extraction_status":{"complete":fal') is None

    def test_an_answer_without_a_status_gives_none(self):
        assert salvage_status('{"rows":[{"a":1},') is None

    def test_a_non_string_gives_none(self):
        assert salvage_status(None) is None


class TestSplitEvenly:
    def test_pages_are_cut_into_equal_consecutive_parts(self):
        assert split_evenly(1, 10, 2) == [(1, 5), (6, 10)]

    def test_never_more_parts_than_pages(self):
        assert split_evenly(4, 5, 3) == [(4, 4), (5, 5)]


class TestRangesFromSalvagedStatus:
    def test_own_pages_are_halved_and_planned_ranges_follow(self):
        status = salvage_status(_CUT_OFF_ANSWER)
        assert ranges_from_salvaged_status(1, 60, status, 60000) == [
            (1, 5),
            (6, 10),
            (11, 30),
            (31, 60),
        ]

    def test_own_pages_are_cut_finer_when_their_estimate_exceeds_the_target(self):
        status = salvage_status(_CUT_OFF_ANSWER)  # own pages: 300k - 240k = 60k
        assert ranges_from_salvaged_status(1, 60, status, 20000)[:3] == [
            (1, 4),
            (5, 8),
            (9, 10),
        ]

    def test_a_status_without_a_usable_last_page_gives_none(self):
        assert ranges_from_salvaged_status(1, 60, {"complete": False}, 60000) is None

    def test_a_single_page_status_that_would_repeat_the_call_gives_none(self):
        status = {"last_pdf_page_extracted": 1, "remaining_page_ranges": []}
        assert ranges_from_salvaged_status(1, 1, status, 60000) is None

    def test_next_ranges_uses_the_salvaged_plan_for_a_failed_call(self):
        status = salvage_status(_CUT_OFF_ANSWER)
        keep, ranges = next_ranges(
            1,
            60,
            None,
            "unexpected end of JSON input in a text answer",
            target=60000,
            salvaged=status,
        )
        assert keep is False
        assert ranges == [(1, 5), (6, 10), (11, 30), (31, 60)]

    def test_next_ranges_falls_back_to_halves_without_a_usable_status(self):
        keep, ranges = next_ranges(
            1, 60, None, "unexpected end of JSON input", target=60000, salvaged={}
        )
        assert ranges == [(1, 30), (31, 60)]


# ---------------------------------------------------------------------------
# Plan-first mode: planning_segments cuts a large file into equal segments that
# fit a part file with one context page; build_plan_predicate asks for ranges
# without extracting; ranges_from_plan maps a planning answer back to pages.
# ---------------------------------------------------------------------------


class TestPlanningSegments:
    def test_a_long_document_is_cut_into_equal_segments_of_at_most_49_pages(self):
        assert planning_segments(192, 8_600_000) == [
            (1, 48),
            (49, 96),
            (97, 144),
            (145, 192),
        ]

    def test_a_large_file_is_cut_by_size(self):
        # 114 pages at ~0.52 MB a page: 18 pages + the context page fit 10 MB.
        segments = planning_segments(114, 59_470_000)
        assert all(b - a + 1 <= 18 for a, b in segments)
        assert segments[0][0] == 1 and segments[-1][1] == 114

    def test_a_short_small_file_is_one_segment(self):
        assert planning_segments(30, 1_000_000) == [(1, 30)]


class TestBuildPlanPredicate:
    def test_a_middle_segment_marks_its_first_position_as_context(self):
        text = build_plan_predicate("Extract.", 49, 96, 192, 60000, {"type": "object"})
        assert "Position 1 is the page BEFORE this segment" in text
        assert "Report positions 2 to 49 of THIS file." in text
        assert "UNDER 60000 output tokens" in text
        assert "Do NOT extract this document" in text

    def test_the_first_segment_has_no_context_page(self):
        text = build_plan_predicate("Extract.", 1, 48, 192, 60000, {"type": "object"})
        assert "Report positions 1 to 48 of THIS file." in text
        assert "BEFORE this segment" not in text


class TestRangesFromPlan:
    def test_positions_of_a_middle_segment_map_back_to_document_pages(self):
        # Segment 49-96: position 2 is page 49, so position k is page k + 47.
        plan = {"ranges": [[2, 10, 50000], [11, 49, 55000]]}
        assert ranges_from_plan(plan, 49, 96) == [(49, 57), (58, 96)]

    def test_a_range_on_the_context_page_alone_is_dropped(self):
        plan = {"ranges": [[1, 1, 100], [2, 49, 55000]]}
        assert ranges_from_plan(plan, 49, 96) == [(49, 96)]

    def test_pages_the_plan_left_out_get_their_own_range(self):
        plan = {"ranges": [[1, 10, 50000], [15, 30, 55000]]}
        assert ranges_from_plan(plan, 1, 30) == [(1, 10), (11, 14), (15, 30)]

    def test_overlapping_ranges_are_trimmed(self):
        plan = {"ranges": [[1, 12, 50000], [10, 20, 55000]]}
        assert ranges_from_plan(plan, 1, 20) == [(1, 12), (13, 20)]

    def test_no_usable_ranges_gives_none(self):
        assert ranges_from_plan({"ranges": [["a", "b"]]}, 1, 20) is None
        assert ranges_from_plan({"pages": []}, 1, 20) is None
        assert ranges_from_plan(None, 1, 20) is None


# ---------------------------------------------------------------------------
# read_answer -- one AI_COMPLETE(..., return_error_details => TRUE) result,
# as a JSON string or an object, to (value, error, salvaged status).
# ---------------------------------------------------------------------------


class TestReadAnswer:
    def test_a_text_answer_is_parsed(self):
        raw = json.dumps({"value": '{"name": "x"}', "error": None})
        assert read_answer(raw) == ({"name": "x"}, None, None)

    def test_an_object_result_is_read_like_a_string_one(self):
        assert read_answer({"value": '{"name": "x"}', "error": None}) == (
            {"name": "x"},
            None,
            None,
        )

    def test_an_ai_complete_error_is_returned_without_parsing_or_salvage(self):
        assert read_answer({"value": None, "error": "malformed request"}) == (
            None,
            "malformed request",
            None,
        )

    def test_a_truncated_answer_salvages_its_status(self):
        text = (
            '{"%s": {"complete": false, "last_pdf_page_extracted": 3}, "rows": [{"a"'
            % (STATUS_FIELD)
        )
        value, error, salvaged = read_answer({"value": text, "error": None})
        assert value is None
        assert error == TRUNCATED_TEXT_ERROR
        assert salvaged == {"complete": False, "last_pdf_page_extracted": 3}

    def test_a_result_that_is_not_an_object_raises(self):
        with pytest.raises(AttributeError):
            read_answer("[1, 2]")


# ---------------------------------------------------------------------------
# is_unknown_model_error / unknown_model_message -- an unknown or unavailable
# model ends the whole read with one clear error; other failures do not.
# ---------------------------------------------------------------------------


class TestUnknownModel:
    def test_the_ai_complete_unknown_model_failure_is_recognized(self):
        error = (
            "Request failed for external function COMPLETE with remote service error: "
            "'400 'unknown model \"claude-x\"''"
        )
        assert is_unknown_model_error(error) is True

    def test_it_is_case_insensitive(self):
        assert is_unknown_model_error("Unknown Model foo") is True

    @pytest.mark.parametrize(
        "error",
        ["429 too many requests", "malformed request", "unexpected end of JSON input"],
    )
    def test_other_failures_are_not_terminal(self, error):
        assert is_unknown_model_error(error) is False

    def test_the_message_names_both_models_and_the_cross_region_setting(self):
        message = no_model_message(["claude-a", "claude-b"])
        assert '"claude-a" or "claude-b"' in message
        assert "CORTEX_ENABLED_CROSS_REGION" in message


# ---------------------------------------------------------------------------
# DocumentExtraction model fallback -- calls go to the first model with no extra
# call; a call AI_COMPLETE reports as unknown-model is not a document failure: the
# read moves on to the next model and sends it again, and stops only when none is left.
# ---------------------------------------------------------------------------

_BUDGET = {"target": 1000, "max_tokens": 100}
_BUDGETS = {
    "first": _BUDGET,
    "range": _BUDGET,
    "large_range": _BUDGET,
    "merge_target": 1000,
    "plan_first": {"range_target": 1000, "max_tokens": 100},
}
_UNKNOWN_TEXT = "Request failed ... 400 'unknown model \"x\"'"
_GOOD = {"value": '{"a": "x"}', "error": None}


class _ModelSession:
    """Answers each AI_COMPLETE submission from ``behave(model, predicate)``, which
    returns a result object or raises."""

    def __init__(self, behave) -> None:
        self.behave = behave
        self.submitted = []

    def sql(self, query, params):
        model, predicate = params[0], params[1]
        self.submitted.append(model)
        behave = self.behave

        class _Job:
            @staticmethod
            def result():
                return [[json.dumps(behave(model, predicate))]]

        class _Query:
            @staticmethod
            def collect_nowait():
                return _Job()

        return _Query()


def _extraction(session, models):
    schema = {"schema": {"type": "object", "properties": {"a": {"type": "string"}}}}
    return DocumentExtraction(session, schema, "p", models, _BUDGETS, "@stage")


def _plain_file(name="@s/f.pdf"):
    return FileState(name, None, 1, 10, 30)


def _run_calls(extraction, files):
    tasks = extraction.plan_large_files(files)
    extraction.extract_until_done(files, tasks, 30)


class TestModelFallback:
    def test_the_first_model_is_used_with_no_extra_call(self):
        session = _ModelSession(lambda model, predicate: _GOOD)
        extraction = _extraction(session, ["m1", "m2"])
        file = _plain_file()
        _run_calls(extraction, [file])
        assert session.submitted == ["m1"]
        assert extraction.model == "m1"
        assert file.calls == 1 and file.errors == [] and len(file.parts) == 1

    @pytest.mark.parametrize("as_exception", [True, False])
    def test_an_unknown_first_model_moves_the_read_to_the_next_one(self, as_exception):
        def behave(model, predicate):
            if model == "m1":
                if as_exception:
                    raise RuntimeError(_UNKNOWN_TEXT)
                return {"value": None, "error": _UNKNOWN_TEXT}
            return _GOOD

        session = _ModelSession(behave)
        extraction = _extraction(session, ["m1", "m2"])
        files = [_plain_file("@s/a.pdf"), _plain_file("@s/b.pdf")]
        _run_calls(extraction, files)
        assert session.submitted == ["m1", "m1", "m2", "m2"]
        assert extraction.model == "m2"
        for file in files:
            assert file.calls == 1
            assert file.errors == [] and len(file.parts) == 1

    def test_calls_failing_with_the_old_model_do_not_move_the_read_on_again(self):
        session = _ModelSession(
            lambda model, predicate: (
                {"value": None, "error": _UNKNOWN_TEXT} if model == "m1" else _GOOD
            )
        )
        extraction = _extraction(session, ["m1", "m2", "m3"])
        files = [
            _plain_file("@s/a.pdf"),
            _plain_file("@s/b.pdf"),
            _plain_file("@s/c.pdf"),
        ]
        _run_calls(extraction, files)
        assert extraction.model == "m2"
        assert session.submitted == ["m1"] * 3 + ["m2"] * 3

    def test_the_read_stops_when_no_model_is_known(self):
        session = _ModelSession(
            lambda model, predicate: {"value": None, "error": _UNKNOWN_TEXT}
        )
        extraction = _extraction(session, ["m1", "m2"])
        with pytest.raises(NoModelAvailable, match="No model is available"):
            _run_calls(extraction, [_plain_file()])
        assert session.submitted == ["m1", "m2"]

    def test_another_error_is_still_a_per_file_error(self):
        session = _ModelSession(
            lambda model, predicate: {"value": None, "error": "boom for good"}
        )
        extraction = _extraction(session, ["m1", "m2"])
        file = _plain_file()
        _run_calls(extraction, [file])
        assert extraction.model == "m1"
        assert session.submitted == ["m1"]
        assert file.calls == 1 and file.parts == []
        assert any("boom for good" in error for error in file.errors)

    def test_planning_calls_are_sent_again_with_the_next_model(self):
        def behave(model, predicate):
            if model == "m1":
                raise RuntimeError(_UNKNOWN_TEXT)
            return {"value": '{"ranges": [[2, 31]]}', "error": None}

        session = _ModelSession(behave)
        extraction = _extraction(session, ["m1", "m2"])
        extraction.stage_part = lambda file, start, end: file.path
        file = FileState("@s/big.pdf", object(), 60, 1000, 30)
        tasks = extraction.plan_large_files([file])
        assert session.submitted == ["m1", "m1", "m2", "m2"]
        assert extraction.model == "m2"
        assert file.calls == 2 and file.errors == []
        assert tasks


# ---------------------------------------------------------------------------
# should_abort / abort_message / document_outcomes -- a read stops early when,
# once enough documents have finished, most of them failed.
# ---------------------------------------------------------------------------


class TestShouldAbort:
    def test_the_thresholds(self):
        assert MIN_DOCUMENTS_BEFORE_ABORT == 10
        assert MAX_FAILED_FRACTION == 0.75

    def test_below_the_minimum_never_aborts(self):
        assert should_abort(9, 9) is False

    def test_the_minimum_with_more_than_the_fraction_failed_aborts(self):
        assert should_abort(10, 8) is True

    def test_exactly_the_fraction_does_not_abort(self):
        assert should_abort(12, 9) is False
        assert should_abort(20, 15) is False

    def test_just_over_the_fraction_aborts(self):
        assert should_abort(12, 10) is True

    def test_few_failures_do_not_abort(self):
        assert should_abort(100, 1) is False

    def test_all_failed_at_the_minimum_aborts(self):
        assert should_abort(10, 10) is True


class TestAbortMessage:
    def test_it_states_the_counts_and_the_most_common_error(self):
        message = abort_message(12, 11, ["boom", "boom", "other"])
        assert "11 of the 12 documents" in message
        assert "the read stopped early" in message
        assert "Most common error: boom" in message

    def test_it_works_without_any_error_text(self):
        assert "Most common error" not in abort_message(12, 11, [])


def _file(name, pages=1, parts=(), errors=()):
    state = FileState(name, object(), pages, 1000, 30)
    state.parts.extend(parts)
    state.errors.extend(errors)
    return state


class TestDocumentOutcomes:
    def test_a_document_with_a_waiting_range_has_not_finished(self):
        waiting = _file("a")
        assert document_outcomes([waiting], [(waiting, 1, 1, "range")]) == (0, 0, [])

    def test_a_document_with_no_successful_part_failed(self):
        failed = _file("a", errors=["boom", "later"])
        assert document_outcomes([failed], []) == (1, 1, ["boom"])

    def test_a_partly_extracted_document_succeeded(self):
        partial = _file("a", parts=[(1, {"x": 1})], errors=["pages 5-9: boom"])
        assert document_outcomes([partial], []) == (1, 0, [])

    def test_a_file_that_is_not_a_pdf_is_not_a_document(self):
        not_a_pdf = FileState("@s/n.txt", None, 1, 10, 30, errors=["boom"])
        assert document_outcomes([not_a_pdf], []) == (0, 0, [])

    def test_a_file_that_could_not_be_read_is_not_a_document(self):
        unreadable = FileState("@s/u.pdf", None, 1, 0, 30, errors=["e"], failed=True)
        assert document_outcomes([unreadable], []) == (0, 0, [])

    def test_ten_failing_pdfs_stay_ten_however_many_non_documents_fail(self):
        files = [_file(f"p{i}", errors=["boom"]) for i in range(10)]
        files += [
            FileState(f"@s/n{i}", None, 1, 10, 30, errors=["x"]) for i in range(5)
        ]
        finished, failed, _ = document_outcomes(files, [])
        assert (finished, failed) == (10, 10)
        assert should_abort(finished, failed)

    def test_failing_non_documents_never_trip_the_stop(self):
        files = [_file(f"p{i}", errors=["boom"]) for i in range(3)]
        files += [
            FileState(f"@s/n{i}", None, 1, 10, 30, errors=["x"]) for i in range(20)
        ]
        finished, failed, _ = document_outcomes(files, [])
        assert (finished, failed) == (3, 3)
        assert not should_abort(finished, failed)


class TestStopWaiting:
    def test_each_file_with_waiting_ranges_gets_one_error_naming_them(self):
        a, b = _file("a", parts=[(1, {"x": 1})], errors=["old"]), _file("b")
        stop_waiting(
            [(a, 5, 9, "range"), (b, 1, 2, "range"), (a, 20, 30, "range")], "why"
        )
        assert a.errors == ["old", "pages 5-9, 20-30 not extracted: why"]
        assert b.errors == ["pages 1-2 not extracted: why"]
        assert a.parts == [(1, {"x": 1})]

    def test_a_file_with_nothing_waiting_is_untouched(self):
        done = _file("done", errors=["e"])
        stop_waiting([(_file("w"), 1, 1, "range")], "why")
        assert done.errors == ["e"]


class _FakeExtraction(DocumentExtraction):
    """No stage, no AI_COMPLETE: a call on a file named "splittable" fails in a way that
    fewer pages can fix, every other call fails for good."""

    def stage_part(self, file, start, end):
        return file.path

    def submit(self, file, predicate, staged, max_tokens):
        file.calls += 1
        return file.path

    @staticmethod
    def collect(job):
        return None, "malformed request" if job == "splittable" else "boom", None


def _run_fake(failing, with_splittable):
    budgets = {
        "first": {"target": 1, "max_tokens": 1},
        "range": {"target": 1, "max_tokens": 1},
        "large_range": {"target": 1, "max_tokens": 1},
        "merge_target": 1,
    }
    schema = {"schema": {"type": "object", "properties": {"a": {"type": "string"}}}}
    extraction = _FakeExtraction(None, schema, "p", ["m"], budgets, "@stage")
    files = [_file(f"f{i}") for i in range(failing)]
    if with_splittable:
        files.append(_file("splittable", pages=4))
    tasks = [(f, 1, f.pages, "range") for f in files]
    extraction.extract_until_done(files, tasks, 30)
    return files


class TestReadStopsWhenMostDocumentsFail:
    def test_it_stops_submitting_and_returns_every_row(self):
        # 11 documents fail for good in round one; the 12th has pages left to retry.
        files = _run_fake(failing=11, with_splittable=True)
        waiting = files[-1]
        assert "not extracted: the read stopped early because 11 of the 11" in (
            waiting.errors[-1]
        )
        assert "pages 1-2, 3-4" in waiting.errors[-1]
        assert files[0].errors == ["pages 1-1: boom"]  # finished rows are untouched

    def test_the_stop_names_the_most_common_failure(self):
        files = _run_fake(failing=11, with_splittable=True)
        assert "Most common error: pages 1-1: boom" in files[-1].errors[-1]

    def test_it_does_not_stop_below_the_minimum(self):
        files = _run_fake(failing=9, with_splittable=True)
        assert all(not f.parts for f in files)
        assert not any("stopped early" in e for f in files for e in f.errors)

    def test_it_has_nothing_to_stop_when_nothing_is_waiting(self):
        # Every document failed, but the read is already complete: the per-document
        # errors are returned as they are.
        files = _run_fake(failing=12, with_splittable=False)
        assert all(f.errors for f in files)
        assert not any("stopped early" in e for f in files for e in f.errors)

    def test_non_pdfs_failing_do_not_stop_the_read(self):
        budgets = {
            "first": {"target": 1, "max_tokens": 1},
            "range": {"target": 1, "max_tokens": 1},
            "large_range": {"target": 1, "max_tokens": 1},
            "merge_target": 1,
        }
        schema = {"schema": {"type": "object", "properties": {"a": {"type": "string"}}}}
        extraction = _FakeExtraction(None, schema, "p", ["m"], budgets, "@stage")
        non_pdfs = [FileState(f"n{i}", None, 1, 10, 30) for i in range(15)]
        waiting = _file("splittable", pages=4)
        files = non_pdfs + [waiting]
        tasks = [(f, 1, f.pages, "range") for f in files]
        extraction.extract_until_done(files, tasks, 30)
        assert all(f.errors for f in non_pdfs)
        assert not any("stopped early" in e for f in files for e in f.errors)


class TestThrottling:
    @pytest.mark.parametrize(
        "error",
        [
            "429 Too Many Requests",
            "Request failed: '429 throttled'",
            "rate limit exceeded",
            "Rate-limit hit",
            "ratelimit",
            "the model is overloaded",
            "503 Service Unavailable",
            "service unavailable",
            "no capacity for the model",
            "insufficient capacity",
        ],
    )
    def test_load_refusals_are_throttling(self, error):
        assert is_throttle_error(error) is True

    @pytest.mark.parametrize(
        "error",
        [
            "malformed request",
            '400 unknown model "x"',
            "unexpected end of JSON input",
            "query 01c800c4-0a1b-e95c-4290-a91512d070ef failed",
            "pages 1-4: boom",
            "the document exceeds the model capacity",
        ],
    )
    def test_other_errors_are_not(self, error):
        assert is_throttle_error(error) is False

    def test_a_throttled_call_is_sent_again_and_succeeds(self, monkeypatch):
        sleeps = []
        monkeypatch.setattr(proc, "_sleep", sleeps.append)
        answers = iter([{"value": None, "error": "429 too many requests"}, _GOOD])
        session = _ModelSession(lambda model, predicate: next(answers))
        extraction = _extraction(session, ["m1"])
        file = _plain_file()
        _run_calls(extraction, [file])
        assert session.submitted == ["m1", "m1"]
        assert file.calls == 1 and file.errors == [] and len(file.parts) == 1
        assert sleeps == [THROTTLE_BACKOFF_SECONDS[0]]

    def test_retries_are_bounded_and_the_last_answer_is_the_error(self, monkeypatch):
        sleeps = []
        monkeypatch.setattr(proc, "_sleep", sleeps.append)
        session = _ModelSession(
            lambda model, predicate: {"value": None, "error": "429 too many requests"}
        )
        extraction = _extraction(session, ["m1"])
        file = _plain_file()
        _run_calls(extraction, [file])
        assert len(session.submitted) == 1 + MAX_THROTTLE_RETRIES
        assert sleeps == list(THROTTLE_BACKOFF_SECONDS[:MAX_THROTTLE_RETRIES])
        assert file.parts == [] and any("429" in e for e in file.errors)
        assert file.calls == 1  # the throttled attempts are not counted

    def test_a_throttled_call_is_not_a_failed_document(self, monkeypatch):
        monkeypatch.setattr(proc, "_sleep", lambda seconds: None)
        extraction = _extraction(_ModelSession(lambda m, p: _GOOD), ["m1"])
        file = _plain_file()
        file.calls = 1
        assert extraction.throttled(file, (id(file), 1, 1), "429 too many requests")
        assert file.calls == 0 and file.errors == [] and extraction.throttle_wait == 5
        extraction.wait_if_throttled()
        assert extraction.throttle_wait == 0

    def test_the_wait_grows_with_each_retry_of_the_same_range(self):
        extraction = _extraction(_ModelSession(lambda m, p: _GOOD), ["m1"])
        file = _plain_file()
        key = (id(file), 1, 1)
        waits = []
        for _ in range(MAX_THROTTLE_RETRIES):
            assert extraction.throttled(file, key, "rate limit")
            waits.append(extraction.throttle_wait)
            extraction.throttle_wait = 0
        assert waits == list(THROTTLE_BACKOFF_SECONDS[:MAX_THROTTLE_RETRIES])
        assert not extraction.throttled(file, key, "rate limit")

    def test_a_throttled_planning_call_is_planned_again(self, monkeypatch):
        sleeps = []
        monkeypatch.setattr(proc, "_sleep", sleeps.append)
        seen = {}

        def behave(model, predicate):
            seen[predicate] = seen.get(predicate, 0) + 1
            if seen[predicate] == 1:  # each segment is throttled once
                return {"value": None, "error": "503 service unavailable"}
            return {"value": '{"ranges": [[2, 31]]}', "error": None}

        session = _ModelSession(behave)
        extraction = _extraction(session, ["m1"])
        extraction.stage_part = lambda file, start, end: file.path
        file = FileState("@s/big.pdf", object(), 60, 1000, 30)
        tasks = extraction.plan_large_files([file])
        # Two segments, both throttled in the same round: one wait, then both again.
        assert len(session.submitted) == 4
        assert sleeps == [THROTTLE_BACKOFF_SECONDS[0]]
        assert file.errors == [] and tasks


# ---------------------------------------------------------------------------
# Per-file failures -- reading, staging, submitting, processing or merging one
# file goes wrong: that file's error, never the whole read. The terminal errors
# (no model; too many documents failed) still stop it.
# ---------------------------------------------------------------------------


_FAKE_PDF_PREFIX = b"%FAKE-PDF pages="


class _FakePdfReader:
    """Reads the bytes ``_FakePdfWriter`` writes; anything else is not a readable PDF."""

    def __init__(self, stream) -> None:
        data = stream.read()
        if not data.startswith(_FAKE_PDF_PREFIX):
            raise ValueError("not a PDF")
        count, _, ids = data[len(_FAKE_PDF_PREFIX) :].partition(b"|")
        self.pages = [int(i) for i in ids.split(b",")] if ids else []
        if len(self.pages) != int(count):
            self.pages = list(range(int(count)))


class _FakePdfWriter:
    def __init__(self) -> None:
        self.pages = []

    def add_page(self, page) -> None:
        self.pages.append(page)

    def add_blank_page(self, width=0, height=0) -> None:
        self.pages.append(len(self.pages))

    def write(self, stream) -> None:
        ids = ",".join(str(page) for page in self.pages)
        stream.write(_FAKE_PDF_PREFIX + f"{len(self.pages)}|{ids}".encode())


@pytest.fixture(autouse=True)
def fake_pypdf(monkeypatch):
    """The procedure imports pypdf, a package of the stored procedure's environment that
    the client's test environment does not install; these tests use a small stand-in."""
    module = types.ModuleType("pypdf")
    module.PdfReader, module.PdfWriter = _FakePdfReader, _FakePdfWriter
    monkeypatch.setitem(sys.modules, "pypdf", module)


def _pdf_bytes(pages):
    writer = _FakePdfWriter()
    for _ in range(pages):
        writer.add_blank_page(width=100, height=100)
    buffer = io.BytesIO()
    writer.write(buffer)
    return buffer.getvalue()


_COMPLETE_ANSWER = json.dumps(
    {
        "value": json.dumps({STATUS_FIELD: {"complete": True}, "a": "x"}),
        "error": None,
    }
)


class _Job:
    def __init__(self, raw) -> None:
        self.raw = raw

    def result(self):
        return [[self.raw]]


class _Result:
    def __init__(self, rows=None, raw=None) -> None:
        self.rows, self.raw = rows or [], raw

    def collect(self):
        return self.rows

    def collect_nowait(self):
        return _Job(self.raw)


class _File:
    """Records every staged part as ``(target, bytes, kwargs)``; raises ``error`` instead
    when one is set."""

    def __init__(self, error=None) -> None:
        self.error, self.staged = error, []

    def put_stream(self, stream, target, **kwargs) -> None:
        if self.error is not None:
            raise self.error
        self.staged.append((target, stream.read(), kwargs))


def _staged_pages(data):
    """The page ids inside a part written by ``_FakePdfWriter``."""
    return _FakePdfReader(io.BytesIO(data)).pages


class _FakeSession:
    """LIST returns ``names``; AI_COMPLETE answers every call with a complete answer."""

    def __init__(self, names=(), submit_error=None, stage_error=None) -> None:
        self.names, self.submit_error = names, submit_error
        self.file = _File(stage_error)

    def sql(self, query, params=None):
        if query.startswith("LIST"):
            return _Result(rows=[{"name": n} for n in self.names])
        if self.submit_error is not None:
            raise self.submit_error
        return _Result(raw=_COMPLETE_ANSWER)


_BUDGETS = {
    "first": {"target": 80000, "max_tokens": 1000},
    "range": {"target": 80000, "max_tokens": 1000},
    "large_range": {"target": 80000, "max_tokens": 1000},
    "merge_target": 60000,
    "plan_first": {"range_target": 60000, "max_tokens": 1000},
}


def _doc_extraction(session):
    schema = {"schema": {"type": "object", "properties": {"a": {"type": "string"}}}}
    return DocumentExtraction(session, schema, "p", ["m1"], _BUDGETS, "@stage")


def _pdf_state(path, pages=4, cap=30):
    data = _pdf_bytes(pages)
    return FileState(path, _FakePdfReader(io.BytesIO(data)), pages, len(data), cap)


class TestPerFileFailures:
    def test_a_file_that_cannot_be_read_is_a_failed_state_and_the_other_loads(
        self, monkeypatch
    ):
        extraction = _doc_extraction(_FakeSession(["s/a.pdf", "s/b.pdf"]))

        def read_bytes(name):
            if name.endswith("a.pdf"):
                raise OSError("denied")
            return _pdf_bytes(3)

        monkeypatch.setattr(extraction, "read_bytes", read_bytes)
        bad, good = extraction.load_files("@s", 30)
        assert bad.failed and bad.errors == ["could not read file: denied"]
        assert not good.failed and good.pages == 3 and good.splittable

    def test_a_file_over_the_read_limit_is_not_read_and_is_a_failed_state(
        self, monkeypatch
    ):
        extraction = _doc_extraction(_FakeSession())
        reads = []
        monkeypatch.setattr(extraction, "read_bytes", lambda name: reads.append(name))
        big = extraction.load_file("@s/big.pdf", 30, MAX_FILE_READ_BYTES + 1)
        assert reads == []
        assert big.failed and big.errors == [
            f"file is {MAX_FILE_READ_BYTES + 1} bytes, larger than the supported "
            f"maximum of {MAX_FILE_READ_BYTES} bytes"
        ]
        assert document_outcomes([big], []) == (0, 0, [])

    def test_a_file_at_the_read_limit_is_read(self, monkeypatch):
        extraction = _doc_extraction(_FakeSession())
        monkeypatch.setattr(extraction, "read_bytes", lambda name: _pdf_bytes(2))
        file = extraction.load_file("@s/a.pdf", 30, MAX_FILE_READ_BYTES)
        assert not file.failed and file.pages == 2

    def test_load_files_passes_the_listed_size(self, monkeypatch):
        class _SizedSession(_FakeSession):
            def sql(self, query, params=None):
                return _Result(
                    rows=[
                        {"name": "s/a.pdf", "size": 10},
                        {"name": "s/b.pdf", "size": MAX_FILE_READ_BYTES + 1},
                    ]
                )

        extraction = _doc_extraction(_SizedSession())
        monkeypatch.setattr(extraction, "read_bytes", lambda name: _pdf_bytes(1))
        small, big = extraction.load_files("@s", 30)
        assert not small.failed and big.failed

    @pytest.mark.parametrize(
        "row, expected",
        [({"size": 12}, 12), ({"size": "34"}, 34), ({}, None), ({"size": None}, None)],
    )
    def test_listed_size(self, row, expected):
        assert listed_size(row) == expected

    def test_a_failed_file_gets_no_tasks_and_still_has_an_output_row(self):
        failed = FileState(
            "@s/a.pdf", None, 1, 0, 30, errors=["could not read file: x"], failed=True
        )
        good = _pdf_state("@s/b.pdf", pages=2)
        tasks = _doc_extraction(_FakeSession()).plan_large_files([failed, good])
        assert [t[0] for t in tasks] == [good]
        assert result_row(failed) == ["@s/a.pdf", None, "could not read file: x", None]

    def test_a_failed_file_is_not_counted_as_a_document(self):
        failed = FileState("@s/a.pdf", None, 1, 0, 30, errors=["e"], failed=True)
        assert document_outcomes([failed], []) == (0, 0, [])

    def test_a_staging_failure_is_that_files_error_and_the_other_file_goes_on(self):
        bad, good = _pdf_state("@s/bad.pdf"), _pdf_state("@s/good.pdf", pages=2)
        tasks = [(bad, 2, 3, "range"), (good, 1, 2, "range")]
        session = _FakeSession(stage_error=OSError("stage is full"))
        _doc_extraction(session).extract_until_done([bad, good], tasks, 30)
        assert bad.parts == []
        assert "could not stage part file: stage is full" in bad.errors[0]
        assert [start for start, _ in good.parts] == [1] and good.errors == []

    def test_a_submit_failure_is_that_files_error(self):
        file = _pdf_state("@s/a.pdf", pages=2)
        session = _FakeSession(submit_error=RuntimeError("no slots"))
        _doc_extraction(session).extract_until_done([file], [(file, 1, 2, "range")], 30)
        assert "could not submit the AI_COMPLETE call: no slots" in file.errors[0]

    def test_an_answer_that_cannot_be_processed_is_that_files_error(self, monkeypatch):
        file = _pdf_state("@s/a.pdf", pages=2)
        extraction = _doc_extraction(_FakeSession())

        def boom(*args, **kwargs):
            raise ValueError("odd answer")

        monkeypatch.setattr(extraction, "finish", boom)
        extraction.extract_until_done([file], [(file, 1, 2, "range")], 30)
        assert file.errors == ["pages 1-2: could not process the answer: odd answer"]

    def test_a_merge_failure_is_that_files_error_with_no_value(self, monkeypatch):
        from snowflake.snowpark._internal import document_extract_proc as proc

        def boom(parts):
            raise ValueError("cannot merge")

        monkeypatch.setattr(proc, "merge_values", boom)
        file = FileState("@s/a.pdf", None, 1, 1, 30, parts=[(1, {"a": 1})])
        assert result_row(file) == [
            "@s/a.pdf",
            None,
            "could not merge the extracted parts: cannot merge",
            None,
        ]

    def test_too_many_failed_documents_stop_the_read_but_keep_every_row(self):
        failed = [_pdf_state(f"@s/{i}.pdf", pages=1) for i in range(10)]
        for file in failed:
            file.errors.append("e")
        waiting = _pdf_state("@s/w.pdf", pages=4)
        remaining = {
            "value": json.dumps(
                {
                    STATUS_FIELD: {
                        "complete": False,
                        "last_pdf_page_extracted": 2,
                        "remaining_page_ranges": [
                            {"start_pdf_page": 3, "end_pdf_page": 4}
                        ],
                    },
                    "a": "x",
                }
            ),
            "error": None,
        }

        class _Partial(_FakeSession):
            def sql(self, query, params=None):
                return _Result(raw=json.dumps(remaining))

        files = failed + [waiting]
        _doc_extraction(_Partial()).extract_until_done(
            files, [(waiting, 1, 4, "range")], 30
        )
        assert waiting.parts and waiting.errors[-1].startswith(
            "pages 3-4 not extracted"
        )
        assert "stopped early because 10 of the 10" in waiting.errors[-1]
        assert [result_row(f)[2] for f in failed] == ["e"] * 10


# ---------------------------------------------------------------------------
# Confidence scores -- an `extraction_scores` property (one number per
# top-level field) is asked for last, kept out of the merged value, merged
# across page ranges by merge_scores, and returned in the SCORES column.
# ---------------------------------------------------------------------------

_SCORED_SCHEMA = {
    "schema": {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "rows": {"type": "array", "items": {"type": "object"}},
        },
    }
}


class TestScoresSchema:
    def test_the_schema_shown_to_the_model_has_one_number_per_top_level_field(self):
        props = with_status(_SCORED_SCHEMA)["schema"]["properties"]
        scores = props[SCORES_FIELD]["properties"]
        assert set(scores) == {"name", "rows"}
        assert all(v == {"type": ["number", "null"]} for v in scores.values())

    def test_status_comes_first_and_scores_last(self):
        names = list(with_status(_SCORED_SCHEMA)["schema"]["properties"])
        assert names == [STATUS_FIELD, "name", "rows", SCORES_FIELD]

    def test_nested_properties_are_not_scored_separately(self):
        nested = {
            "schema": {
                "type": "object",
                "properties": {
                    "a": {"type": "object", "properties": {"b": {"type": "string"}}}
                },
            }
        }
        scores = with_status(nested)["schema"]["properties"][SCORES_FIELD]
        assert set(scores["properties"]) == {"a"}

    def test_a_schema_without_properties_gets_no_scores_field(self):
        props = with_status({"schema": {"type": "object"}})["schema"]["properties"]
        assert SCORES_FIELD not in props

    def test_scores_schema_lists_the_given_fields(self):
        assert set(scores_schema(["x", "y"])["properties"]) == {"x", "y"}

    def test_the_prompt_asks_for_the_scores_last(self):
        sent = with_status(_SCORED_SCHEMA)["schema"]
        text = schema_instruction(sent)
        assert "Fill extraction_scores last" in text
        assert "number from 0 to 1" in text
        assert "really absent" in text

    def test_the_prompt_does_not_mention_scores_without_the_property(self):
        assert "extraction_scores" not in schema_instruction({"type": "object"})


class TestScoresStayOutOfTheValue:
    PART = {
        STATUS_FIELD: {"complete": True},
        "name": "a",
        SCORES_FIELD: {"name": 0.9},
    }

    def test_merge_values_drops_the_scores(self):
        assert merge_values([self.PART]) == {"name": "a"}

    def test_scores_alone_are_not_content(self):
        assert is_empty_answer({SCORES_FIELD: {"name": 0.5}}) is True
        assert is_empty_answer({"name": "a", SCORES_FIELD: {"name": 0.5}}) is False

    def test_shifting_page_fields_leaves_the_scores_alone(self):
        value = {"rows": [{"page": 2}], SCORES_FIELD: {"rows": 0.7, "page": 0.5}}
        out = shift_page_fields(value, 5, 9)
        assert out[SCORES_FIELD] == {"rows": 0.7, "page": 0.5}
        assert out["rows"] == [{"page": 5}]


class TestMergeScores:
    def test_one_part_returns_its_scores(self):
        part = {"a": "x", "b": [1], SCORES_FIELD: {"a": 0.9, "b": 0.4}}
        assert merge_scores([part]) == {"a": 0.9, "b": 0.4}

    def test_a_field_takes_the_minimum_over_the_parts_that_have_a_value(self):
        parts = [
            {"a": "x", SCORES_FIELD: {"a": 0.9}},
            {"a": "y", SCORES_FIELD: {"a": 0.6}},
            {"a": None, SCORES_FIELD: {"a": 0.99}},
        ]
        assert merge_scores(parts) == {"a": 0.6}

    def test_a_field_empty_everywhere_takes_the_maximum(self):
        parts = [
            {"a": None, SCORES_FIELD: {"a": 0.7}},
            {"a": "", SCORES_FIELD: {"a": 0.95}},
        ]
        assert merge_scores(parts) == {"a": 0.95}

    def test_scores_are_clamped_to_zero_and_one(self):
        part = {"a": "x", "b": "y", SCORES_FIELD: {"a": 1.7, "b": -0.2}}
        assert merge_scores([part]) == {"a": 1.0, "b": 0.0}

    @pytest.mark.parametrize(
        "bad", ["0.9", True, None, [0.9], float("nan"), float("inf")]
    )
    def test_unusable_scores_are_left_out(self, bad):
        assert merge_scores([{"a": "x", SCORES_FIELD: {"a": bad}}]) == {}

    def test_a_part_without_scores_or_with_a_non_object_is_skipped(self):
        parts = [
            {"a": "x"},
            {"a": "y", SCORES_FIELD: "high"},
            {"a": "z", SCORES_FIELD: {"a": 0.8}},
        ]
        assert merge_scores(parts) == {"a": 0.8}

    def test_names_outside_the_schema_fields_are_ignored(self):
        part = {"a": "x", SCORES_FIELD: {"a": 0.9, "made_up": 0.1}}
        assert merge_scores([part], fields=["a"]) == {"a": 0.9}

    def test_no_parts_gives_no_scores(self):
        assert merge_scores([]) == {}


class TestResultRowScores:
    def test_the_row_carries_the_merged_scores_as_json(self):
        file = FileState(
            "@s/a.pdf",
            None,
            1,
            1,
            30,
            parts=[(1, {"a": "x", SCORES_FIELD: {"a": 0.8, "zzz": 0.1}})],
        )
        row = result_row(file, ["a"])
        assert json.loads(row[1]) == {"a": "x"}
        assert json.loads(row[3]) == {"a": 0.8}

    def test_a_file_whose_answer_had_no_scores_has_a_null_scores_column(self):
        file = FileState("@s/a.pdf", None, 1, 1, 30, parts=[(1, {"a": "x"})])
        assert result_row(file, ["a"])[3] is None


# ---------------------------------------------------------------------------
# DocumentExtraction.stage_part -- a page range becomes its own part file on the
# work stage, with the previous page in front as context.
# ---------------------------------------------------------------------------


def _part_target(path, start, end):
    digest = hashlib.md5(path.encode()).hexdigest()[:12]
    stem = path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    return f"@stage/document_parts/{digest}/{stem}_p{start}_{end}.pdf"


class TestStagePart:
    def test_a_middle_range_is_staged_with_the_previous_page_as_context(self):
        session = _FakeSession()
        file = _pdf_state("@s/a.pdf", pages=6)
        target = _doc_extraction(session).stage_part(file, 3, 5)
        assert target == _part_target("@s/a.pdf", 3, 5)
        [(staged_to, data, kwargs)] = session.file.staged
        assert staged_to == target
        # Positions 2-5: page 2 is the context page in front of pages 3-5.
        assert _staged_pages(data) == [1, 2, 3, 4]
        assert kwargs == {"auto_compress": False, "overwrite": True}

    def test_a_range_from_the_first_page_has_no_context_page(self):
        session = _FakeSession()
        file = _pdf_state("@s/a.pdf", pages=6)
        _doc_extraction(session).stage_part(file, 1, 2)
        [(_, data, _)] = session.file.staged
        assert _staged_pages(data) == [0, 1]

    def test_the_whole_small_file_is_sent_as_it_is(self):
        session = _FakeSession()
        file = _pdf_state("@s/a.pdf", pages=6)
        assert _doc_extraction(session).stage_part(file, 1, 6) == "@s/a.pdf"
        assert session.file.staged == []

    def test_a_file_that_is_not_a_pdf_is_sent_as_it_is(self):
        session = _FakeSession()
        file = FileState("@s/a.txt", None, 1, 10, 30)
        assert _doc_extraction(session).stage_part(file, 1, 1) == "@s/a.txt"
        assert session.file.staged == []

    def test_a_part_still_over_the_size_limit_is_not_staged(self, monkeypatch):
        monkeypatch.setattr(proc, "MAX_FILE_BYTES", 5)
        session = _FakeSession()
        file = _pdf_state("@s/a.pdf", pages=6)
        assert _doc_extraction(session).stage_part(file, 2, 4) is None
        assert session.file.staged == []

    def test_a_single_page_over_the_size_limit_is_still_staged(self, monkeypatch):
        monkeypatch.setattr(proc, "MAX_FILE_BYTES", 5)
        session = _FakeSession()
        file = _pdf_state("@s/a.pdf", pages=6)
        target = _doc_extraction(session).stage_part(file, 4, 4)
        assert target == _part_target("@s/a.pdf", 4, 4)
        assert len(session.file.staged) == 1


class TestReadBytes:
    def test_it_reads_the_staged_file(self, monkeypatch):
        import snowflake.snowpark.files as files

        opened = []

        class _Opened:
            def __enter__(self):
                return io.BytesIO(b"content")

            def __exit__(self, *args):
                return False

        def fake_open(name, mode, **kwargs):
            opened.append((name, mode, kwargs))
            return _Opened()

        monkeypatch.setattr(files.SnowflakeFile, "open", staticmethod(fake_open))
        assert DocumentExtraction.read_bytes("@s/a.pdf") == b"content"
        assert opened == [("@s/a.pdf", "rb", {"require_scoped_url": False})]


# ---------------------------------------------------------------------------
# run -- the stored procedure end to end, with a fake session: LIST, reading the
# files, AI_COMPLETE answers per staged file, part staging and the output table.
# ---------------------------------------------------------------------------


def _answer(value=None, error=None):
    return json.dumps(
        {"value": json.dumps(value) if value is not None else None, "error": error}
    )


class _RunSession:
    """LIST returns ``files``' names; AI_COMPLETE answers come from ``answer(staged)``,
    keyed by the staged path each call was sent; ``create_dataframe`` returns its
    arguments."""

    def __init__(self, files, answer) -> None:
        self.files, self.answer = files, answer
        self.file = _File()
        self.calls = []

    def sql(self, query, params=None):
        if query.startswith("LIST"):
            return _Result(rows=[{"name": f"s/{name}"} for name in self.files])
        self.calls.append(params[2])
        return _Result(raw=self.answer(params[2]))

    def create_dataframe(self, rows, schema):
        return rows, schema


_RUN_SCHEMA = {
    "schema": {
        "type": "object",
        "properties": {
            "invoice": {"type": "string"},
            "rows": {
                "type": "array",
                "items": {"type": "object", "properties": {"n": {"type": "integer"}}},
            },
        },
    }
}


def _run(monkeypatch, files, answer):
    session = _RunSession(files, answer)
    monkeypatch.setattr(
        DocumentExtraction,
        "read_bytes",
        staticmethod(lambda name: session.files[name.rsplit("/", 1)[-1]]),
    )
    rows, schema = proc.run(
        session,
        "@s",
        json.dumps(_RUN_SCHEMA),
        "Extract.",
        json.dumps(["m1"]),
        json.dumps(_BUDGETS),
        30,
        "@stage",
    )
    return session, rows, schema


class TestRun:
    def test_one_row_per_file_with_values_errors_and_scores(self, monkeypatch):
        def answer(staged):
            if staged == "@s/good.pdf":
                return _answer(
                    {
                        STATUS_FIELD: {"complete": True},
                        "invoice": "INV-1",
                        "rows": [{"n": 1}],
                        SCORES_FIELD: {"invoice": 0.9, "rows": 0.8},
                    }
                )
            return _answer(error="boom")

        files = {"good.pdf": _pdf_bytes(1), "bad.pdf": _pdf_bytes(1)}
        _, rows, schema = _run(monkeypatch, files, answer)
        assert [field.name for field in schema.fields] == [
            "STAGE_PATH",
            "VALUE",
            "ERROR",
            "SCORES",
        ]
        by_path = {row[0]: row for row in rows}
        good, bad = by_path["@s/good.pdf"], by_path["@s/bad.pdf"]
        assert json.loads(good[1]) == {"invoice": "INV-1", "rows": [{"n": 1}]}
        assert good[2] is None
        assert json.loads(good[3]) == {"invoice": 0.9, "rows": 0.8}
        assert bad[1] is None and "boom" in bad[2] and bad[3] is None

    def test_a_file_that_does_not_fit_is_split_and_merged(self, monkeypatch):
        def answer(staged):
            if staged == "@s/long.pdf":
                status = {
                    "complete": False,
                    "last_pdf_page_extracted": 2,
                    "remaining_page_ranges": [{"start_pdf_page": 3, "end_pdf_page": 4}],
                }
                return _answer(
                    {STATUS_FIELD: status, "invoice": "INV-2", "rows": [{"n": 1}]}
                )
            assert staged == _part_target("@s/long.pdf", 3, 4)
            return _answer(
                {STATUS_FIELD: {"complete": True}, "invoice": None, "rows": [{"n": 2}]}
            )

        session, rows, _ = _run(monkeypatch, {"long.pdf": _pdf_bytes(4)}, answer)
        [(path, value, error, _)] = rows
        assert path == "@s/long.pdf" and error is None
        assert json.loads(value) == {"invoice": "INV-2", "rows": [{"n": 1}, {"n": 2}]}
        assert session.calls == ["@s/long.pdf", _part_target("@s/long.pdf", 3, 4)]
        # The part for pages 3-4 carries page 2 in front as context.
        [(_, data, _)] = session.file.staged
        assert _staged_pages(data) == [1, 2, 3]


# ---------------------------------------------------------------------------
# Small branches of the helpers that the tests above do not reach.
# ---------------------------------------------------------------------------


class TestHelperBranches:
    def test_the_planning_prompt_for_a_whole_document(self):
        prompt = build_plan_predicate("p", 1, 5, 5, 1000, {"type": "object"})
        assert "This file is the whole document: pages 1 to 5" in prompt

    def test_next_ranges_skips_unusable_planned_ranges(self):
        status = {
            "complete": False,
            "last_pdf_page_extracted": 2,
            "remaining_page_ranges": [
                "not a range",
                {"start_pdf_page": None, "end_pdf_page": 4},
                {"start_pdf_page": 1, "end_pdf_page": 1},
                {"start_pdf_page": 3, "end_pdf_page": 5},
            ],
        }
        keep, ranges = next_ranges(1, 5, {STATUS_FIELD: status}, None)
        assert keep is True
        assert ranges == [(3, 5)]

    def test_cover_skips_a_range_already_covered(self):
        assert _cover([(1, 4), (2, 3), (5, 6)], 1, 6) == [(1, 4), (5, 6)]

    def test_merge_keeps_the_first_value_when_the_second_is_absent(self):
        assert merge_values([{"a": "x"}, {"a": None}]) == {"a": "x"}

    def test_a_truncated_answer_with_escaped_quotes_is_still_truncated(self):
        value, error = parse_text_answer('{"a": "say \\"hi\\" ok", "b": [1, 2')
        assert value is None
        assert error == TRUNCATED_TEXT_ERROR

    def test_a_salvaged_status_skips_unusable_planned_ranges(self):
        status = {
            "last_pdf_page_extracted": 2,
            "estimated_total_output_tokens": 300,
            "remaining_page_ranges": [
                "not a range",
                {"start_pdf_page": None, "end_pdf_page": 4},
                {"start_pdf_page": 1, "end_pdf_page": 1},
                {
                    "start_pdf_page": 3,
                    "end_pdf_page": 6,
                    "estimated_output_tokens": "many",
                },
            ],
        }
        ranges = ranges_from_salvaged_status(1, 6, status, 100)
        assert ranges[-1] == (3, 6)
        assert ranges[0][0] == 1

    def test_brackets_inside_strings_and_escaped_quotes_are_ignored(self):
        assert _brackets_close('{"a": "has } and \\" inside"}') is True
        assert _brackets_close('{"a": "has { and \\" inside"') is False


class TestExtractionPaths:
    def test_a_file_that_is_not_a_pdf_is_one_unsplittable_page(self, monkeypatch):
        extraction = _doc_extraction(_FakeSession(["s/a.txt"]))
        monkeypatch.setattr(extraction, "read_bytes", lambda name: b"plain text")
        [file] = extraction.load_files("@s", 30)
        assert file.reader is None and file.pages == 1 and not file.splittable

    def test_a_non_pdf_keeps_its_answer_or_its_error(self, monkeypatch):
        def answer(staged):
            if staged == "@s/ok.txt":
                return _answer({STATUS_FIELD: {"complete": True}, "invoice": "I"})
            return _answer({})

        files = {"ok.txt": b"text", "empty.txt": b"text"}
        _, rows, _ = _run(monkeypatch, files, answer)
        by_path = {row[0]: row for row in rows}
        assert json.loads(by_path["@s/ok.txt"][1]) == {"invoice": "I"}
        assert by_path["@s/empty.txt"][1] is None
        assert by_path["@s/empty.txt"][2] == EMPTY_ANSWER_ERROR

    def test_a_planning_segment_that_cannot_be_staged_falls_back_to_a_first_call(
        self,
    ):
        session = _FakeSession(stage_error=OSError("stage is full"))
        large = _pdf_state("@s/big.pdf", pages=BOUNDED_FIRST_CALL_MIN_PAGES + 10)
        tasks = _doc_extraction(session).plan_large_files([large])
        assert tasks and all(budget == "first" for _, _, _, budget in tasks)
        assert large.calls == 0

    def test_a_file_at_its_call_cap_gets_an_error_and_no_call(self):
        session = _FakeSession()
        file = _pdf_state("@s/a.pdf", pages=2, cap=0)
        _doc_extraction(session).extract_until_done([file], [(file, 1, 2, "range")], 0)
        assert file.errors == ["pages 1-2 not extracted: reached 0 calls"]
        assert file.parts == []

    def test_a_part_still_too_large_is_cut_in_half(self, monkeypatch):
        monkeypatch.setattr(proc, "MAX_FILE_BYTES", 5)
        session = _FakeSession()
        file = _pdf_state("@s/a.pdf", pages=4)
        _doc_extraction(session).extract_until_done([file], [(file, 1, 4, "range")], 30)
        # Pages 1-4 and 1-2 are too large; single pages are staged and answered.
        assert [start for start, _ in file.parts] == [1, 2, 3, 4]
        assert file.errors == []

    def test_a_large_estimate_raises_the_files_call_cap(self, monkeypatch):
        def answer(staged):
            return _answer(
                {
                    STATUS_FIELD: {
                        "complete": True,
                        "estimated_total_output_tokens": 10_000_000,
                    },
                    "invoice": "I",
                }
            )

        session = _RunSession({"a.pdf": _pdf_bytes(1)}, answer)
        extraction = _doc_extraction(session)
        file = _pdf_state("@s/a.pdf", pages=1, cap=3)
        extraction.extract_until_done([file], [(file, 1, 1, "range")], 3)
        assert file.cap > 3
