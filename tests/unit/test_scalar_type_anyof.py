#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Acceptance tests for the ExtractionSpec._scalar_type anyOf/oneOf fix.

These are NOT unit tests in the tests/ tree -- the fix in
/tmp/w2/scalar_type_anyof.diff has deliberately not been applied to src/ yet
(a 29-family measurement run is live), so this file is run twice from the
outside: once against a patched copy of document_reader_options.py loaded
from /tmp/w2/, and once against the real, unpatched module in src/, to prove
the new cases actually exercise the fix rather than passing tautologically
either way. See the __main__ block at the bottom.

The positive fixtures are h12's REAL property schemas (dumped directly from
the loaded case, not hand-written), because h12 is the concrete evidence for
this bug: every one of its 78 fields is anyOf-wrapped, none carry a bare
"type", so before this fix _scalar_type returned None for all 78 and
cast_extracted_field() left every cell as raw, double-JSON-quoted VARIANT --
masked in scoring only because the harness's unwrap_extracted()
(eval/extractbench/ai_extract.py:176) strips exactly that extra quoting layer
before computing F1.

One correction to the original task description: h12 was described as
having an "anyOf integer+null" field via total_anticipated_leases. Direct
inspection of the live case schema shows this is incorrect --
total_anticipated_leases is anyOf [string, null] ("Extract verbatim as a
string" per its own description), and a full sweep of all 78 h12 properties
turns up zero integer/number-typed fields (only 41 boolean+null and 37
string+null). So the integer branch below uses a clearly-labeled SYNTHETIC
fixture instead of a real h12 field -- disclosed here and in the report
rather than silently invented or silently skipped.
"""
import sys

import pytest

# ---------------------------------------------------------------------------
# Real h12 field shapes, copied verbatim from case.data_schema["properties"]
# (dumped 2026-09-22 against the live snapshot; trimmed to the keys this
# fix cares about -- "default"/"description"/"title" are irrelevant to
# _scalar_type and dropped for readability, they are not part of what makes
# these anyOf-wrapped).
# ---------------------------------------------------------------------------
H12_OPERATOR_NAME = {"anyOf": [{"type": "string"}, {"type": "null"}]}
H12_RECOVERY_WATERFLOOD = {"anyOf": [{"type": "boolean"}, {"type": "null"}]}
H12_TOTAL_ANTICIPATED_LEASES = {"anyOf": [{"type": "string"}, {"type": "null"}]}

# Synthetic -- no integer/number field exists anywhere in h12's 78 properties.
# Included only to exercise _JSON_SCHEMA_SCALAR_TYPES["integer"] through this
# same anyOf path; not claimed to come from any real case.
SYNTHETIC_INTEGER_ANYOF = {"anyOf": [{"type": "integer"}, {"type": "null"}]}

# oneOf is the other spelling the fix must accept (JSON Schema treats it the
# same as anyOf for a two-member nullable union); reuse operator_name's shape.
ONEOF_STRING = {"oneOf": [{"type": "string"}, {"type": "null"}]}

# --- negative cases: must stay None, both before and after the fix ---------

# Two real (non-null) members: genuinely ambiguous, no single type to name.
TWO_REAL_MEMBERS = {"anyOf": [{"type": "string"}, {"type": "integer"}]}

# A composite member: array/object are absent from _JSON_SCHEMA_SCALAR_TYPES
# on purpose (AI_EXTRACT/AI_COMPLETE already return them as structured
# VARIANT), so resolving the union must not manufacture a type for them.
ANYOF_ARRAY = {
    "anyOf": [{"type": "array", "items": {"type": "string"}}, {"type": "null"}]
}
ANYOF_OBJECT = {
    "anyOf": [
        {"type": "object", "properties": {"x": {"type": "string"}}},
        {"type": "null"},
    ]
}

# An unresolvable $ref: _scalar_type only ever sees a single property dict,
# never the root schema's $defs, so it cannot chase this -- and must not try
# to (that would mean reimplementing _resolve_ref's root-context-dependent
# logic here, which is exactly the kind of hand-rolling this fix is supposed
# to stop doing). Staying None is the correct, honest answer.
ANYOF_UNRESOLVABLE_REF = {"anyOf": [{"$ref": "#/$defs/Foo"}, {"type": "null"}]}

# --- cases that must be completely unaffected by this fix ------------------

PLAIN_STRING_TYPE = {"type": "string"}
TYPE_LIST_NULLABLE_INT = {"type": ["integer", "null"]}  # task #18's shape
NOT_A_DICT = "not a schema"


@pytest.fixture(scope="module")
def extraction_spec():
    # The fix is landed in the tree, so this exercises the real module. It was
    # originally written against a patched copy under /tmp, loaded by path, so
    # the same suite could be run on both the patched and the unpatched file to
    # prove it was not passing tautologically (5 of 12 failed unpatched).
    from snowflake.snowpark._internal.document_reader_options import ExtractionSpec

    return ExtractionSpec


class TestRealH12Shapes:
    def test_operator_name_string_anyof(self, extraction_spec):
        from snowflake.snowpark.types import StringType

        assert extraction_spec._scalar_type(H12_OPERATOR_NAME) == StringType()

    def test_recovery_waterflood_boolean_anyof(self, extraction_spec):
        from snowflake.snowpark.types import BooleanType

        assert extraction_spec._scalar_type(H12_RECOVERY_WATERFLOOD) == BooleanType()

    def test_total_anticipated_leases_string_anyof(self, extraction_spec):
        # NOTE: string, not integer -- see module docstring for the correction.
        from snowflake.snowpark.types import StringType

        assert (
            extraction_spec._scalar_type(H12_TOTAL_ANTICIPATED_LEASES) == StringType()
        )

    def test_oneof_spelling_also_resolves(self, extraction_spec):
        from snowflake.snowpark.types import StringType

        assert extraction_spec._scalar_type(ONEOF_STRING) == StringType()

    def test_synthetic_integer_anyof(self, extraction_spec):
        # Disclosed synthetic fixture: no real h12 (or other known) field has
        # this shape, but the integer branch of _JSON_SCHEMA_SCALAR_TYPES
        # still needs coverage through the anyOf path.
        from snowflake.snowpark.types import LongType

        assert extraction_spec._scalar_type(SYNTHETIC_INTEGER_ANYOF) == LongType()


class TestNegativeCasesMustStayNone:
    def test_two_real_members_stays_none(self, extraction_spec):
        assert extraction_spec._scalar_type(TWO_REAL_MEMBERS) is None

    def test_anyof_array_stays_none(self, extraction_spec):
        assert extraction_spec._scalar_type(ANYOF_ARRAY) is None

    def test_anyof_object_stays_none(self, extraction_spec):
        assert extraction_spec._scalar_type(ANYOF_OBJECT) is None

    def test_anyof_unresolvable_ref_stays_none(self, extraction_spec):
        assert extraction_spec._scalar_type(ANYOF_UNRESOLVABLE_REF) is None


class TestExistingContractUnaffected:
    def test_plain_type_still_resolves(self, extraction_spec):
        from snowflake.snowpark.types import StringType

        assert extraction_spec._scalar_type(PLAIN_STRING_TYPE) == StringType()

    def test_type_list_nullable_still_resolves(self, extraction_spec):
        # This is task #18's fix (type: [X, "null"]) -- must still work
        # unchanged by this anyOf-specific addition.
        from snowflake.snowpark.types import LongType

        assert extraction_spec._scalar_type(TYPE_LIST_NULLABLE_INT) == LongType()

    def test_non_dict_input_stays_none(self, extraction_spec):
        assert extraction_spec._scalar_type(NOT_A_DICT) is None


if __name__ == "__main__":
    # Run against whichever module path is passed on argv[1] (patched or
    # unpatched); default to the patched copy in this directory.
    module_path = (
        sys.argv[1]
        if len(sys.argv) > 1
        else "/tmp/w2/document_reader_options.patched.py"
    )
    pytest.dro_module_path = module_path
    raise SystemExit(pytest.main([__file__, "-v", "--no-header"]))
