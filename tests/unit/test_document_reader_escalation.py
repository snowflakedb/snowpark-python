#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from unittest.mock import MagicMock

import pytest

from snowflake.snowpark._internal import document_reader
from snowflake.snowpark._internal.document_reader import _STAGE_PATH_COLUMN
from snowflake.snowpark._internal.document_reader_options import (
    DocumentReaderOptions,
    ExtractionSpec,
)


def _extraction() -> ExtractionSpec:
    return ExtractionSpec(
        fields=["invoice_number"],
        field_columns=["INVOICE_NUMBER"],
        ai_extract_format=None,
        ai_complete_format=None,
        field_types={"invoice_number": None},
    )


# ---------------------------------------------------------------------------
# escalation_eligible -- the escalate-on-failure retry only ever makes sense
# for the one combination measured to actually need it (ai_complete at
# document boundary, with a real schema and no page_filter already narrowing
# things); every other combination must stay False.
# ---------------------------------------------------------------------------


class TestEscalationEligible:
    def test_eligible_combination_returns_true(self):
        options = DocumentReaderOptions(
            extraction=_extraction(),
            extraction_engine="ai_complete",
            row_boundary="document",
            page_filter=None,
        )
        assert document_reader.escalation_eligible(options) is True

    def test_no_schema_returns_false(self):
        options = DocumentReaderOptions(
            extraction=None,
            extraction_engine="ai_complete",
            row_boundary="document",
            page_filter=None,
        )
        assert document_reader.escalation_eligible(options) is False

    def test_ai_extract_engine_returns_false(self):
        options = DocumentReaderOptions(
            extraction=_extraction(),
            extraction_engine="ai_extract",
            row_boundary="document",
            page_filter=None,
        )
        assert document_reader.escalation_eligible(options) is False

    def test_row_boundary_already_page_returns_false(self):
        options = DocumentReaderOptions(
            extraction=_extraction(),
            extraction_engine="ai_complete",
            row_boundary="page",
            page_filter=None,
        )
        assert document_reader.escalation_eligible(options) is False

    def test_page_filter_set_returns_false(self):
        options = DocumentReaderOptions(
            extraction=_extraction(),
            extraction_engine="ai_complete",
            row_boundary="document",
            page_filter=[{"start": 0, "end": 1}],
        )
        assert document_reader.escalation_eligible(options) is False


# ---------------------------------------------------------------------------
# build_extraction_plan -- only_stage_paths is the mechanism escalation uses to
# scope the page-boundary retry to just the documents that failed; when it's
# not given, the plan must be identical to today's (no filter at all).
# ---------------------------------------------------------------------------


class TestBuildExtractionPlan:
    def test_no_filter_when_only_stage_paths_is_none(self, monkeypatch):
        fake_df = MagicMock(name="files_df")
        monkeypatch.setattr(document_reader, "access", lambda session, path: fake_df)
        options = DocumentReaderOptions(parse_mode="none", extraction=None)

        result = document_reader.build_extraction_plan(MagicMock(), "@stage", options)

        fake_df.filter.assert_not_called()
        assert result is fake_df

    def test_no_filter_when_only_stage_paths_is_empty(self, monkeypatch):
        fake_df = MagicMock(name="files_df")
        monkeypatch.setattr(document_reader, "access", lambda session, path: fake_df)
        options = DocumentReaderOptions(parse_mode="none", extraction=None)

        result = document_reader.build_extraction_plan(
            MagicMock(), "@stage", options, only_stage_paths=[]
        )

        fake_df.filter.assert_not_called()
        assert result is fake_df

    def test_filter_added_when_only_stage_paths_given(self, monkeypatch):
        fake_df = MagicMock(name="files_df")
        filtered_df = MagicMock(name="filtered_df")
        fake_df.filter.return_value = filtered_df
        monkeypatch.setattr(document_reader, "access", lambda session, path: fake_df)
        options = DocumentReaderOptions(parse_mode="none", extraction=None)

        result = document_reader.build_extraction_plan(
            MagicMock(), "@stage", options, only_stage_paths=["a/b.pdf"]
        )

        fake_df.filter.assert_called_once()
        assert result is filtered_df


# ---------------------------------------------------------------------------
# escalate_failed_documents -- no live session is exercised here; `plan` and
# its downstream chain (cache_result/filter/select/distinct/collect,
# union_all_by_name) are all mocked, and build_extraction_plan/
# aggregate_extracted_pages are monkeypatched so only escalate_failed_documents'
# own control flow (which documents to retry, and what options to retry them
# with) is under test.
# ---------------------------------------------------------------------------


class TestEscalateFailedDocuments:
    def test_no_failures_returns_cached_unchanged(self):
        options = DocumentReaderOptions(
            extraction=_extraction(),
            extraction_engine="ai_complete",
            row_boundary="document",
        )
        plan = MagicMock(name="plan")
        cached = MagicMock(name="cached")
        plan.cache_result.return_value = cached
        cached.filter.return_value.select.return_value.distinct.return_value.collect.return_value = (
            []
        )

        result = document_reader.escalate_failed_documents(
            MagicMock(), "@stage", options, plan
        )

        assert result is cached
        cached.union_all_by_name.assert_not_called()

    @pytest.mark.parametrize(
        "given_parse_mode,expected_parse_mode",
        [
            ("none", "layout"),
            ("layout", "layout"),
            ("ocr", "ocr"),
            ("text", "text"),
        ],
    )
    def test_page_options_row_boundary_and_parse_mode(
        self, monkeypatch, given_parse_mode, expected_parse_mode
    ):
        captured = {}

        def fake_build_extraction_plan(session, path, options, only_stage_paths=None):
            captured["page_options"] = options
            captured["only_stage_paths"] = only_stage_paths
            return MagicMock(name="page_plan")

        monkeypatch.setattr(
            document_reader, "build_extraction_plan", fake_build_extraction_plan
        )
        monkeypatch.setattr(
            document_reader,
            "aggregate_extracted_pages",
            lambda *args, **kwargs: MagicMock(name="recovered"),
        )

        options = DocumentReaderOptions(
            extraction=_extraction(),
            extraction_engine="ai_complete",
            row_boundary="document",
            parse_mode=given_parse_mode,
        )
        plan = MagicMock(name="plan")
        cached = MagicMock(name="cached")
        plan.cache_result.return_value = cached
        cached.filter.return_value.select.return_value.distinct.return_value.collect.return_value = [
            {_STAGE_PATH_COLUMN: "stage/failing.pdf"}
        ]

        document_reader.escalate_failed_documents(MagicMock(), "@stage", options, plan)

        page_options = captured["page_options"]
        assert page_options.row_boundary == "page"
        assert page_options.parse_mode == expected_parse_mode
        # The original options object must be left untouched -- dataclasses.replace
        # returns a new instance rather than mutating options in place.
        assert options.row_boundary == "document"
        assert options.parse_mode == given_parse_mode
        assert captured["only_stage_paths"] == ["stage/failing.pdf"]

    def test_recovered_rows_are_unioned_with_the_non_failing_rows(self, monkeypatch):
        monkeypatch.setattr(
            document_reader,
            "build_extraction_plan",
            lambda *args, **kwargs: MagicMock(name="page_plan"),
        )
        recovered = MagicMock(name="recovered")
        monkeypatch.setattr(
            document_reader,
            "aggregate_extracted_pages",
            lambda *args, **kwargs: recovered,
        )

        options = DocumentReaderOptions(
            extraction=_extraction(),
            extraction_engine="ai_complete",
            row_boundary="document",
        )
        plan = MagicMock(name="plan")
        cached = MagicMock(name="cached")
        plan.cache_result.return_value = cached
        cached.filter.return_value.select.return_value.distinct.return_value.collect.return_value = [
            {_STAGE_PATH_COLUMN: "stage/failing.pdf"}
        ]
        succeeding = MagicMock(name="succeeding_rows")
        cached.filter.return_value = succeeding
        # The first cached.filter(...) call above (for the failing-paths query)
        # and this second one (for the succeeding rows) both go through
        # cached.filter, so both return the same mock -- fine, since only the
        # final union_all_by_name call is asserted here.
        succeeding.select.return_value.distinct.return_value.collect.return_value = [
            {_STAGE_PATH_COLUMN: "stage/failing.pdf"}
        ]

        result = document_reader.escalate_failed_documents(
            MagicMock(), "@stage", options, plan
        )

        succeeding.union_all_by_name.assert_called_once_with(recovered)
        assert result is succeeding.union_all_by_name.return_value
