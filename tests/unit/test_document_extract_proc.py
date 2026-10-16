#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import copy

import pytest

from snowflake.snowpark._internal.document_extract_proc import (
    BOUNDED_FIRST_CALL_MIN_BYTES,
    BOUNDED_FIRST_CALL_MIN_PAGES,
    CHARS_PER_OUTPUT_TOKEN,
    EMPTY_ANSWER_ERROR,
    HARD_MAX_CALLS_PER_FILE,
    MAX_FILE_BYTES,
    STATUS_FIELD,
    _cover,
    _to_document_page,
    build_predicate,
    call_cap,
    coalesce_ranges,
    continues_across_split,
    correction_factor,
    drop_unsupported,
    first_round_budget,
    initial_ranges,
    is_empty_answer,
    is_splittable_error,
    merge_values,
    next_ranges,
    nullable_scalars,
    observed_factor,
    qualify_listed_name,
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
# max_tokens); an input Cortex rejects as too large (claude-opus-5 reports
# that only as "malformed request"); or an answer too long to generate, which
# json mode can return as an empty object with no error at all (see
# is_empty_answer and EMPTY_ANSWER_ERROR below). Any other error is not.
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
# initial_ranges -- page ranges for the first round of calls: the whole file
# when it already fits in one call, otherwise even ranges sized from the
# average page size so each part (after the first, which also carries one
# context page) stays under the byte limit Cortex accepts for a FILE input.
# ---------------------------------------------------------------------------


class TestInitialRanges:
    def test_file_within_the_byte_limit_is_a_single_range(self):
        assert initial_ranges(66, 216_000) == [(1, 66)]

    def test_file_exactly_at_the_byte_limit_is_a_single_range(self):
        # file_bytes == max_bytes still fits in one call.
        assert initial_ranges(10, MAX_FILE_BYTES) == [(1, 10)]

    def test_single_page_file_is_a_single_range_even_if_oversized(self):
        # total_pages <= 1: there is nothing left to split into.
        assert initial_ranges(1, 20_000_000) == [(1, 1)]

    def test_oversized_file_is_split_into_even_ranges_from_the_average_page_size(self):
        assert initial_ranges(66, 15_190_000) == [(1, 51), (52, 66)]

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
# unwrap_envelope -- claude-opus-5 sometimes nests its whole answer under one
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
        # Seen live: claude-opus-5 sometimes names the envelope after the SQL
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

    def test_missing_status_block_is_treated_as_complete(self):
        keep, ranges = next_ranges(1, 10, {"name": "x"}, None)
        assert keep is True
        assert ranges == []

    def test_complete_true_or_missing_is_treated_as_complete(self):
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

    # -- factor/target: without them, behavior is unchanged from above; with them,
    # the covered ranges are passed through coalesce_ranges using each planned
    # range's own estimated_output_tokens (see TestCoalesceRanges).

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

    def test_omitting_factor_and_target_keeps_the_covered_ranges_uncoalesced(self):
        keep, ranges = next_ranges(1, 50, self.REMAINING_WITH_ESTIMATES, None)
        assert keep is True
        assert ranges == [(10, 20), (21, 30), (31, 40), (41, 50)]

    def test_factor_and_target_coalesce_the_covered_ranges(self):
        keep, ranges = next_ranges(
            1, 50, self.REMAINING_WITH_ESTIMATES, None, factor=0.5, target=40000
        )
        assert keep is True
        # Same shape as the TestCoalesceRanges example: the first three ranges'
        # corrected estimates (15000 each) merge two at a time under the 40000
        # target, and the last range (no estimate) never merges.
        assert ranges == [(10, 30), (31, 40), (41, 50)]

    def test_supplying_only_one_of_factor_or_target_keeps_ranges_uncoalesced(self):
        keep, ranges = next_ranges(
            1, 50, self.REMAINING_WITH_ESTIMATES, None, factor=0.5
        )
        assert keep is True
        assert ranges == [(10, 20), (21, 30), (31, 40), (41, 50)]


# ---------------------------------------------------------------------------
# coalesce_ranges -- the model's own plan is far finer than the output-token
# target asks for (it overestimates by 1.2-2.6x), so adjacent ranges are
# merged while their corrected (estimate * factor) sum stays within target; a
# range with no estimate is never merged into, or merged from.
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
        assert coalesce_ranges(ranges, 0.5, 40000) == [(10, 30), (31, 40), (41, 50)]

    def test_a_none_estimate_range_is_never_merged_into_by_the_next_range(self):
        # The None-estimate range is appended on its own; the following range's own
        # scaled estimate can't be added onto a None running total either.
        ranges = [(1, 5, None), (6, 10, 1000)]
        assert coalesce_ranges(ranges, 1.0, 100_000) == [(1, 5), (6, 10)]

    def test_a_single_range_passes_through_unchanged(self):
        assert coalesce_ranges([(1, 5, 1000)], 1.0, 100_000) == [(1, 5)]

    def test_empty_ranges_gives_an_empty_list(self):
        assert coalesce_ranges([], 1.0, 100_000) == []

    def test_merging_stops_once_the_target_would_be_exceeded(self):
        # 20000 * 1.0 + 20000 * 1.0 = 40000 <= 40000 merges; the third range's own
        # 20000 would push the running total to 60000, over target, so it starts a
        # new group instead.
        ranges = [(1, 10, 20000), (11, 20, 20000), (21, 30, 20000)]
        assert coalesce_ranges(ranges, 1.0, 40000) == [(1, 20), (21, 30)]


# ---------------------------------------------------------------------------
# observed_factor -- actual output tokens (answer length / CHARS_PER_OUTPUT_TOKEN)
# divided by the tokens the model estimated it would need for the pages it
# actually finished (its own total minus whatever it still planned, when not
# complete). None when there is no usable estimate to compare against.
# ---------------------------------------------------------------------------


class TestObservedFactor:
    def test_complete_call_divides_by_the_total_estimate(self):
        value = {
            STATUS_FIELD: {"complete": True, "estimated_total_output_tokens": 100},
            "name": "hello",
        }
        # len(json.dumps({"name": "hello"})) == 17.
        assert observed_factor(value) == pytest.approx(
            17 / CHARS_PER_OUTPUT_TOKEN / 100
        )

    def test_incomplete_call_subtracts_the_remaining_ranges_estimates(self):
        value = {
            STATUS_FIELD: {
                "complete": False,
                "estimated_total_output_tokens": 1000,
                "remaining_page_ranges": [{"estimated_output_tokens": 400}],
            },
            "name": "hello",
        }
        # done = 1000 - 400 = 600; len(json.dumps({"name": "hello"})) == 17.
        assert observed_factor(value) == pytest.approx(
            17 / CHARS_PER_OUTPUT_TOKEN / 600
        )

    def test_non_int_estimates_in_remaining_ranges_are_ignored(self):
        value = {
            STATUS_FIELD: {
                "complete": False,
                "estimated_total_output_tokens": 1000,
                "remaining_page_ranges": [
                    {"estimated_output_tokens": True},
                    {"estimated_output_tokens": "100"},
                    {},
                    "not a dict",
                ],
            },
            "name": "hello",
        }
        # None of the remaining entries contribute, so done == the total.
        assert observed_factor(value) == pytest.approx(
            17 / CHARS_PER_OUTPUT_TOKEN / 1000
        )

    def test_done_at_or_below_zero_is_none(self):
        value = {
            STATUS_FIELD: {
                "complete": False,
                "estimated_total_output_tokens": 100,
                "remaining_page_ranges": [{"estimated_output_tokens": 150}],
            },
            "name": "hello",
        }
        assert observed_factor(value) is None

    @pytest.mark.parametrize("total", [None, 0, -5])
    def test_missing_zero_or_negative_total_is_none(self, total):
        value = {
            STATUS_FIELD: {"complete": True, "estimated_total_output_tokens": total}
        }
        assert observed_factor(value) is None

    def test_bool_total_is_none(self):
        value = {
            STATUS_FIELD: {"complete": True, "estimated_total_output_tokens": True}
        }
        assert observed_factor(value) is None

    def test_missing_status_block_is_none(self):
        assert observed_factor({"name": "hello"}) is None

    def test_non_dict_status_is_none(self):
        assert observed_factor({STATUS_FIELD: "not a dict"}) is None

    @pytest.mark.parametrize("value", [None, "not a dict", 42, []])
    def test_non_dict_value_is_none(self, value):
        assert observed_factor(value) is None

    def test_the_status_blocks_own_size_does_not_affect_the_answer_length(self):
        # Same answer field ("name") and the same estimate either way -- a bigger
        # status block (more remaining_page_ranges bookkeeping) must not change the
        # measured answer length, since the status block is stripped first.
        small_status = {"complete": True, "estimated_total_output_tokens": 100}
        big_status = {**small_status, "padding": "x" * 1000}
        assert observed_factor(
            {STATUS_FIELD: small_status, "name": "hello"}
        ) == observed_factor({STATUS_FIELD: big_status, "name": "hello"})


# ---------------------------------------------------------------------------
# correction_factor -- the median of a file's own observed factors so far,
# clamped to [0.2, 1.5]; 1.0 (trust the model) until there is anything to
# correct it with.
# ---------------------------------------------------------------------------


class TestCorrectionFactor:
    def test_empty_list_is_1_0(self):
        assert correction_factor([]) == 1.0

    def test_single_value_within_range_is_returned_as_is(self):
        assert correction_factor([0.8]) == 0.8

    def test_odd_length_uses_the_middle_value(self):
        assert correction_factor([0.9, 0.5, 1.3]) == 0.9

    def test_even_length_uses_the_mean_of_the_middle_two(self):
        assert correction_factor([0.6, 0.4]) == pytest.approx(0.5)

    def test_clamped_to_the_floor(self):
        assert correction_factor([0.05]) == 0.2

    def test_clamped_to_the_ceiling(self):
        assert correction_factor([2.0]) == 1.5


# ---------------------------------------------------------------------------
# call_cap -- the per-file AI_COMPLETE call budget: twice the corrected number
# of target-sized ranges the estimated answer needs, plus slack, never below
# the starting floor nor above HARD_MAX_CALLS_PER_FILE.
# ---------------------------------------------------------------------------


class TestCallCap:
    @pytest.mark.parametrize(
        "floor, estimated_total, factor, range_target, expected",
        [
            (12, 900_000, 0.5, 40_000, 28),
            (12, 5_000, 1.0, 40_000, 12),
            (12, 1_400_000, 0.6, 40_000, 46),
        ],
    )
    def test_worked_examples(
        self, floor, estimated_total, factor, range_target, expected
    ):
        assert call_cap(floor, estimated_total, factor, range_target) == expected

    def test_a_huge_estimate_is_capped_at_the_hard_max(self):
        assert call_cap(12, 100_000_000, 1.0, 1000) == HARD_MAX_CALLS_PER_FILE

    @pytest.mark.parametrize("estimated_total", [None, 0])
    def test_a_falsy_estimated_total_returns_the_floor_unchanged(self, estimated_total):
        assert call_cap(12, estimated_total, 0.5, 40_000) == 12


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
