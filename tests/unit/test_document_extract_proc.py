#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import copy

import pytest

from snowflake.snowpark._internal.document_extract_proc import (
    EMPTY_ANSWER_ERROR,
    MAX_FILE_BYTES,
    STATUS_FIELD,
    _cover,
    _to_document_page,
    build_predicate,
    drop_unsupported,
    initial_ranges,
    is_empty_answer,
    is_splittable_error,
    merge_values,
    next_ranges,
    nullable_root_scalars,
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
        # The root scalar property is made nullable (see TestNullableRootScalars).
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

    def test_root_scalars_are_nullable_after_wrapping(self):
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
        # object-typed root properties, and anything nested inside them, are untouched.
        assert properties["address"]["type"] == "object"
        assert properties["address"]["properties"]["city"]["type"] == "string"
        # The status block's own fields report real types, not nullable ones -- it is
        # self-reported by the model, not a value that could be missing from its pages.
        assert properties[STATUS_FIELD]["properties"]["complete"]["type"] == "boolean"


# ---------------------------------------------------------------------------
# nullable_root_scalars -- a call covering only some pages can't see a value
# printed elsewhere, and a non-null scalar type then tempts the model into a
# placeholder like "<UNKNOWN>" that json mode rejects outright; make every
# TOP-LEVEL scalar-typed property nullable so a legitimate "not on these
# pages" answer is just null.
# ---------------------------------------------------------------------------


class TestNullableRootScalars:
    @pytest.mark.parametrize("json_type", ["string", "integer", "number", "boolean"])
    def test_scalar_types_become_nullable(self, json_type):
        properties = {"value": {"type": json_type}}
        assert nullable_root_scalars(properties) == {
            "value": {"type": [json_type, "null"]}
        }

    @pytest.mark.parametrize("json_type", ["object", "array", "null"])
    def test_object_array_and_null_types_are_untouched(self, json_type):
        properties = {"value": {"type": json_type}}
        assert nullable_root_scalars(properties) == properties

    def test_already_list_typed_property_is_untouched(self):
        properties = {"value": {"type": ["string", "integer"]}}
        assert nullable_root_scalars(properties) == properties

    def test_untyped_property_is_untouched(self):
        properties = {"value": {"description": "no type given"}}
        assert nullable_root_scalars(properties) == properties

    def test_non_dict_property_is_untouched(self):
        properties = {"value": True}
        assert nullable_root_scalars(properties) == {"value": True}

    def test_nested_properties_are_untouched(self):
        # Only TOP-LEVEL properties are made nullable; this function never recurses.
        properties = {
            "address": {
                "type": "object",
                "properties": {"city": {"type": "string"}},
            }
        }
        result = nullable_root_scalars(properties)
        assert result["address"]["type"] == "object"
        assert result["address"]["properties"]["city"] == {"type": "string"}

    def test_does_not_mutate_the_input(self):
        properties = {"name": {"type": "string"}}
        original = copy.deepcopy(properties)
        nullable_root_scalars(properties)
        assert properties == original


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
# merge_values -- folds the per-range answers (given in page order) back into
# one object: arrays concatenate, nested objects merge field by field, scalars
# keep the first present value, and extraction_status never leaks into the
# final answer.
# ---------------------------------------------------------------------------


class TestMergeValues:
    def test_arrays_concatenate_in_order(self):
        parts = [{"items": [1, 2]}, {"items": [3, 4]}]
        assert merge_values(parts) == {"items": [1, 2, 3, 4]}

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
