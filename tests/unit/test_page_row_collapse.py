#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

#
# Copyright (c) 2012-2026 Snowflake Computing Inc. All rights reserved.
#

"""The reader collapses page rows it chose itself, and only those.

row_boundary="page" is a legitimate request -- one row per page is the shape
retrieval chunking wants -- so a caller who asks for it must still get N rows.
The collapse exists for the other case: infer_row_boundary() picking page rows
on the caller's behalf for a document-level schema, which returns rows where
each carries only the fields its own page showed and none is the answer.
"""

import pytest

from snowflake.snowpark._internal.document_reader import collapse_inferred_page_rows
from snowflake.snowpark._internal.document_reader_heuristics import (
    apply_document_heuristics,
)
from snowflake.snowpark._internal.document_reader_options import (
    DocumentReaderOptions,
    ExtractionSpec,
)

# A row-shaped schema whose field names trip _PAGE_SCOPE_TOKENS, which is how
# infer_row_boundary() reaches "page" -- the same route W2-27 takes, the one
# family in the short split where the planner infers page boundary.
PAGE_SCOPED_SCHEMA = {
    "type": "object",
    "properties": {
        "page_number": {"type": ["integer", "null"]},
        "well_name": {"type": ["string", "null"]},
    },
}
FLAT_SCHEMA = {
    "type": "object",
    "properties": {
        "invoice_number": {"type": ["string", "null"]},
        "total": {"type": ["number", "null"]},
    },
}


def _options(**kwargs) -> DocumentReaderOptions:
    options = DocumentReaderOptions(
        extraction=ExtractionSpec.from_response_format(FLAT_SCHEMA)
    )
    for key, value in kwargs.items():
        setattr(options, key, value)
    return options


class TestCollapsePredicate:
    def test_inferred_page_rows_collapse(self):
        options = _options(
            row_boundary="page", row_boundary_inferred=True, parse_mode="layout"
        )
        assert collapse_inferred_page_rows(options) is True

    def test_caller_requested_page_rows_are_left_alone(self):
        # The whole point of the flag: identical output shape, opposite handling,
        # because the caller asked for this one.
        options = _options(
            row_boundary="page", row_boundary_inferred=False, parse_mode="layout"
        )
        assert collapse_inferred_page_rows(options) is False

    def test_document_boundary_never_collapses(self):
        options = _options(
            row_boundary="document", row_boundary_inferred=True, parse_mode="layout"
        )
        assert collapse_inferred_page_rows(options) is False

    def test_no_schema_never_collapses(self):
        # Parse-only reads have no field columns to aggregate, and page rows of
        # CONTENT are exactly what a text-extraction caller wants.
        options = _options(
            row_boundary="page", row_boundary_inferred=True, parse_mode="layout"
        )
        options.extraction = None
        assert collapse_inferred_page_rows(options) is False

    def test_parse_disabled_never_collapses(self):
        # page_rows is False when parse_mode="none", since there is no parse to
        # split; guards against the predicate firing on a FILE passthrough.
        options = _options(
            row_boundary="page", row_boundary_inferred=True, parse_mode="none"
        )
        assert options.page_rows is False
        assert collapse_inferred_page_rows(options) is False


class TestPlannerRecordsWhoChose:
    def test_planner_marks_its_own_boundary_choice(self):
        options = apply_document_heuristics(
            DocumentReaderOptions(), explicit={"SCHEMA"}, schema=PAGE_SCOPED_SCHEMA
        )
        assert options.row_boundary == "page"
        assert options.row_boundary_inferred is True
        assert collapse_inferred_page_rows(options) is True

    def test_explicit_row_boundary_is_not_marked_inferred(self):
        options = DocumentReaderOptions(row_boundary="page")
        options = apply_document_heuristics(
            options,
            explicit={"SCHEMA", "ROW_BOUNDARY"},
            schema=PAGE_SCOPED_SCHEMA,
        )
        assert options.row_boundary == "page"
        assert options.row_boundary_inferred is False
        assert collapse_inferred_page_rows(options) is False

    def test_inferred_document_boundary_is_marked_but_does_not_collapse(self):
        # The flag records who chose, not what was chosen -- so it is True here
        # too, and the predicate's page_rows check is what keeps this row-per-file.
        options = apply_document_heuristics(
            DocumentReaderOptions(), explicit={"SCHEMA"}, schema=FLAT_SCHEMA
        )
        assert options.row_boundary == "document"
        assert options.row_boundary_inferred is True
        assert collapse_inferred_page_rows(options) is False

    def test_default_is_not_inferred(self):
        # Nothing should collapse on a bare DocumentReaderOptions -- the flag has
        # to be set deliberately by the planner.
        assert DocumentReaderOptions().row_boundary_inferred is False


class TestMutuallyExclusiveWithEscalation:
    def test_escalation_and_collapse_cannot_both_apply(self):
        from snowflake.snowpark._internal.document_reader import escalation_eligible

        # escalation_eligible requires row_boundary == "document"; the collapse
        # requires page rows. No options object can satisfy both, so read_documents'
        # if/elif is not hiding an ordering bug.
        for boundary in ("document", "page"):
            options = _options(
                row_boundary=boundary,
                row_boundary_inferred=True,
                parse_mode="layout",
                extraction_engine="ai_complete",
            )
            assert not (
                escalation_eligible(options) and collapse_inferred_page_rows(options)
            )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
