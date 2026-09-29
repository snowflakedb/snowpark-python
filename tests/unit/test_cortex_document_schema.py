#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

#
# Copyright (c) 2012-2026 Snowflake Computing Inc. All rights reserved.
#

"""Schema-preparation tests for cortex_document_schema.

The nullability tests exist because dropping the ``null`` arm of an
``anyOf[X, null]`` silently turned every optional field into a mandatory one.
Real documents leave optional fields blank, the model reported null, and Cortex
JSON mode rejected the **entire** response -- so one empty phone number on an
invoice discarded a whole extraction. 10 of 57 ExtractBench families failed this
way.

Verified against live Cortex JSON mode (2026-09-22): both
``anyOf: [{"type": "string"}, {"type": "null"}]`` and ``type: ["string", "null"]``
are accepted and return a real JSON null, including on a nested object leaf.
"""

import os
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "eval"))

# extractbench.ai_extract re-execs the interpreter at import; short-circuit it.
os.environ.setdefault("_EXTRACTBENCH_LIBS_BOOTSTRAPPED", "1")

from snowflake.snowpark._internal.cortex_document_schema import (  # noqa: E402
    for_cortex_complete,
    for_cortex_extract,
    is_extract_legal,
    simplify_json_schema,
)


def nullable(defn):
    """Does this property definition admit a null value?

    Accepts either spelling -- `type: [X, "null"]` (what we emit) or
    `anyOf: [{X}, {"null"}]` (what callers write) -- so the assertions test the
    behaviour rather than the encoding.
    """
    if not isinstance(defn, dict):
        return False
    if "anyOf" in defn:
        return any(
            isinstance(b, dict) and b.get("type") == "null" for b in defn["anyOf"]
        )
    json_type = defn.get("type")
    if isinstance(json_type, list):
        return "null" in json_type
    return json_type == "null"


def non_null_types(defn):
    """The non-null type(s) a definition allows, under either spelling."""
    if not isinstance(defn, dict):
        return set()
    if "anyOf" in defn:
        return {b.get("type") for b in defn["anyOf"] if isinstance(b, dict)} - {"null"}
    json_type = defn.get("type")
    if isinstance(json_type, list):
        return set(json_type) - {"null"}
    return {json_type}


def leaf(prepared, *path):
    node = prepared
    for step in path:
        node = node["properties"][step]
    return node


# The real shape from short/aclu_cdwg_invoice, reduced to what matters.
INVOICE_SCHEMA = {
    "type": "object",
    "properties": {
        "invoice_number": {"type": "string", "description": "Invoice number."},
        "vendor": {
            "type": "object",
            "description": "The party issuing the invoice.",
            "properties": {
                "name": {"type": "string", "description": "Vendor name."},
                "phone": {
                    "anyOf": [{"type": "string"}, {"type": "null"}],
                    "description": "Vendor phone as printed. Null if not present.",
                },
                "address": {
                    "type": "object",
                    "properties": {
                        "city": {"type": "string"},
                        "country": {
                            "anyOf": [{"type": "string"}, {"type": "null"}],
                            "description": "Country. Null if not present.",
                        },
                    },
                },
            },
        },
        "tax_total": {
            "anyOf": [{"type": "number"}, {"type": "null"}],
            "description": "Total tax. Null if not shown.",
        },
        "line_items": {
            "anyOf": [
                {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {"sku": {"type": "string"}},
                    },
                },
                {"type": "null"},
            ],
            "description": "Invoice line items.",
        },
    },
}


class TestNullabilitySurvivesPreparation:
    def test_top_level_nullable_scalar_stays_nullable(self):
        prepared = for_cortex_complete(INVOICE_SCHEMA)
        assert nullable(leaf(prepared, "tax_total")), (
            "a nullable number must still admit null after stringification; "
            "otherwise the model's correct null is rejected"
        )

    def test_nested_nullable_leaf_stays_nullable(self):
        prepared = for_cortex_complete(INVOICE_SCHEMA)
        assert nullable(leaf(prepared, "vendor", "phone"))

    def test_doubly_nested_nullable_leaf_stays_nullable(self):
        """/vendor/address/country -- the exact path Cortex rejected."""
        prepared = for_cortex_complete(INVOICE_SCHEMA)
        assert nullable(leaf(prepared, "vendor", "address", "country"))

    def test_non_nullable_field_does_not_become_nullable(self):
        """The fix must add null only where the caller allowed it."""
        prepared = for_cortex_complete(INVOICE_SCHEMA)
        assert not nullable(leaf(prepared, "invoice_number"))
        assert not nullable(leaf(prepared, "vendor", "name"))
        assert not nullable(leaf(prepared, "vendor", "address", "city"))

    def test_nullable_array_stays_nullable(self):
        prepared = for_cortex_complete(INVOICE_SCHEMA)
        assert nullable(leaf(prepared, "line_items"))

    def test_scalars_keep_their_real_type_by_default(self):
        """Real types are the default: stringifying them discarded whole responses.

        A `number` leaf sent as `string` makes json mode reject the ENTIRE reply when the
        model answers with a real number -- one dollar amount lost every other field on 32
        documents (eval/cortex_raw/STRINGIFICATION.md).
        """
        prepared = for_cortex_complete(INVOICE_SCHEMA)
        types = non_null_types(leaf(prepared, "tax_total"))
        assert "number" in types, "a number leaf must stay a number"

    def test_stringification_is_still_reachable_for_rollback(self):
        """The legacy path must remain available if a customer schema regresses."""
        prepared = for_cortex_complete(INVOICE_SCHEMA, stringify_scalars=True)
        types = non_null_types(leaf(prepared, "tax_total"))
        assert "number" not in types
        assert "string" in types

    def test_nullability_survives_the_legacy_path_too(self):
        prepared = for_cortex_complete(INVOICE_SCHEMA, stringify_scalars=True)
        assert nullable(leaf(prepared, "tax_total"))


class TestArrayItemsSurvive:
    """The nested-array delivery defect: `items` must not be destroyed.

    `_scalars_as_string` deleted an array's `items` and moved the item schema into the
    description as prose, so the array was no longer declared an array at all. Nested
    arrays then reached the caller as escaped JSON strings -- `census_statab` scored
    0.0563 delivered against 1.0000 produced.
    """

    def test_array_declares_items_by_default(self):
        prepared = for_cortex_complete(INVOICE_SCHEMA)
        line_items = leaf(prepared, "line_items")
        blob = json.dumps(line_items)
        assert '"items"' in blob, "array lost its items declaration"
        assert "JSON array of" not in blob, "item schema leaked into the description"

    def test_nested_array_inside_an_array_item_stays_an_array(self):
        schema = {
            "type": "object",
            "properties": {
                "tables": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "column_headers": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
                            "rows": {
                                "type": "array",
                                "items": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                },
                            },
                        },
                    },
                }
            },
        }
        prepared = for_cortex_complete(schema)
        item = prepared["properties"]["tables"]["items"]
        assert item["properties"]["column_headers"]["type"] == "array"
        assert item["properties"]["rows"]["type"] == "array"
        assert item["properties"]["rows"]["items"]["type"] == "array"


class TestRequiredIsNotForwarded:
    """Cortex enforces `required` as must-be-non-null, not "key present".

    One field the model cannot find discarded the whole extraction with
    `root level "invoice_number" value is required` (eval/cortex_raw/REQUIRED_FIELDS.md).
    """

    def test_required_is_stripped_by_default(self):
        prepared = for_cortex_complete(INVOICE_SCHEMA)
        assert "required" not in json.dumps(prepared)

    def test_nested_required_is_stripped_too(self):
        schema = {
            "type": "object",
            "required": ["a"],
            "properties": {
                "a": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["x"],
                        "properties": {"x": {"type": "string"}},
                    },
                }
            },
        }
        assert "required" not in json.dumps(for_cortex_complete(schema))

    def test_required_can_be_forwarded_for_rollback(self):
        schema = {
            "type": "object",
            "required": ["a"],
            "properties": {"a": {"type": "string"}},
        }
        prepared = for_cortex_complete(schema, forward_required=True)
        assert prepared["required"] == ["a"]

    def test_nullable_leaves_still_declare_a_type(self):
        """Cortex's INPUT validator requires a `type` on every property.

        A bare ``anyOf`` is rejected with "please specify a valid json schema
        object ('type' missing?...)" -- claude-4-sonnet enforces this even though
        claude-haiku-4-5 tolerates the union form, so omitting `type` would leave
        the payload valid on only one model.
        """
        prepared = for_cortex_complete(INVOICE_SCHEMA)
        for path in (
            ("tax_total",),
            ("vendor", "phone"),
            ("vendor", "address", "country"),
            ("line_items",),
        ):
            defn = leaf(prepared, *path)
            assert "type" in defn, f"{'.'.join(path)} has no type key"

    def test_no_private_markers_leak_into_the_payload(self):
        """Whatever bookkeeping the fix uses must not reach Cortex."""
        import json

        text = json.dumps(for_cortex_complete(INVOICE_SCHEMA))
        for marker in ("__nullable__", "_nullable", "nullable_mark"):
            assert marker not in text, f"internal marker {marker!r} leaked"

    def test_descriptions_are_preserved(self):
        prepared = for_cortex_complete(INVOICE_SCHEMA)
        phone = leaf(prepared, "vendor", "phone")
        blob = json.dumps(phone) if not isinstance(phone, str) else phone
        assert "Null if not present" in blob

    @pytest.mark.parametrize("spelling", ["anyOf", "type_list"])
    def test_either_accepted_spelling_is_recognised(self, spelling):
        """Both forms validated against live Cortex; helper must accept both."""
        if spelling == "anyOf":
            defn = {"anyOf": [{"type": "string"}, {"type": "null"}]}
        else:
            defn = {"type": ["string", "null"]}
        assert nullable(defn)


class TestPreparationInvariantsHold:
    def test_for_cortex_extract_is_unchanged_by_the_fix(self):
        """The EXTRACT payload must still ship refs un-inlined.

        Cortex rejects the inlined form, so for_cortex_extract deliberately does
        not simplify. Only the COMPLETE path gains nullability.
        """
        schema = {
            "type": "object",
            "$defs": {
                "Row": {"type": "object", "properties": {"a": {"type": "string"}}}
            },
            "properties": {
                "rows": {
                    "anyOf": [
                        {"type": "array", "items": {"$ref": "#/$defs/Row"}},
                        {"type": "null"},
                    ]
                },
            },
        }
        prepared = for_cortex_extract(schema)
        assert "$defs" in prepared
        assert prepared["properties"]["rows"]["anyOf"][0]["items"] == {
            "$ref": "#/$defs/Row"
        }

    def test_legality_walk_still_rejects_ref_wrapped_object_arrays(self):
        """The earlier routing fix must not regress."""
        schema = {
            "type": "object",
            "$defs": {
                "Row": {"type": "object", "properties": {"a": {"type": "string"}}}
            },
            "properties": {
                "rows": {
                    "anyOf": [
                        {"type": "array", "items": {"$ref": "#/$defs/Row"}},
                        {"type": "null"},
                    ]
                },
            },
        }
        assert is_extract_legal(schema) is False

    def test_legality_walk_still_accepts_nullable_scalars(self):
        schema = {
            "type": "object",
            "properties": {
                "note": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                "total": {"anyOf": [{"type": "number"}, {"type": "null"}]},
            },
        }
        assert is_extract_legal(schema) is True

    def test_simplify_without_stringify_also_keeps_nullability(self):
        prepared = simplify_json_schema(INVOICE_SCHEMA)
        assert nullable(leaf(prepared, "vendor", "phone"))


import json  # noqa: E402  (used in a test above)
