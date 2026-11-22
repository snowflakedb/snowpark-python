#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import io
import json
from unittest import mock

import pytest

import snowflake.snowpark._internal.document_extract_proc as proc
from snowflake.snowpark._internal.document_extract_proc import (
    NOT_SENT_TO_IMAGE_MODEL_ERROR,
    DocumentExtraction,
    FieldExample,
    FileState,
    ImageExtraction,
    ModelScore,
    ModelSelection,
    cap_budgets,
    field_agreement,
    field_optimizer_prompt,
    find_excerpts,
    leaves,
    normalize_scalar,
    pages_text,
    read_models,
    result_row,
    sample_files,
    sample_size,
    score_model,
    select_model,
    selection_summary,
    unquoted,
    value_texts,
    with_descriptions,
)
from snowflake.snowpark._internal.document_reader import (
    _CALL_BUDGETS,
    _EXTRACTION_MODELS,
    _MODEL_SELECTION,
)
from snowflake.snowpark._internal.document_reader_options import (
    DocumentReaderOptions,
)

_SCHEMA = {
    "type": "object",
    "properties": {
        "invoice": {"type": "string", "description": "The invoice number."},
        "total": {"type": "number"},
        "rows": {
            "type": "array",
            "items": {"type": "object", "properties": {"n": {"type": "integer"}}},
        },
    },
}
_FORMAT = {"type": "json", "schema": _SCHEMA}
_FIELDS = ["invoice", "total", "rows"]
_REFERENCE = _MODEL_SELECTION["reference"]
_TIERS = _MODEL_SELECTION["tiers"]
_DEFAULT_PROMPT = "Extract."


def _state(path, value, size=1):
    """A finished FileState answering ``value``, or failed when it is None."""
    state = FileState(path, None, 1, size, 30)
    if value is None:
        state.errors.append("failed")
    else:
        state.parts.append((1, value))
        state.usage = {"prompt_tokens": 100, "completion_tokens": 10}
    return state


def _not_sent(path, size=1):
    return FileState.failed_with(path, size, 30, NOT_SENT_TO_IMAGE_MODEL_ERROR)


def _pdf(pages):
    from pypdf import PdfWriter

    writer = PdfWriter()
    for _ in range(pages):
        writer.add_blank_page(width=100, height=100)
    buffer = io.BytesIO()
    writer.write(buffer)
    return buffer.getvalue()


def _rows(values):
    return {
        path: result_row(_state(path, value), _FIELDS) for path, value in values.items()
    }


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------


class TestNormalizeScalar:
    @pytest.mark.parametrize(
        "value, expected",
        [
            ("  Acme   Corp ", "acme corp"),
            ("1,234.50", 1234.5),
            (1234.5, 1234.5),
            (3, 3.0),
            (True, "true"),
            ("", None),
            ("   ", None),
            ("INV-001", "inv-001"),
        ],
    )
    def test_normalizes(self, value, expected):
        assert normalize_scalar(value) == expected

    def test_a_boolean_never_equals_a_number(self):
        assert normalize_scalar(True) != normalize_scalar(1)


class TestLeaves:
    def test_paths_name_keys_but_not_array_positions(self):
        assert leaves({"a": [{"n": 1}, {"n": 2}], "b": "x"}) == [
            (".a[].n", 1.0),
            (".a[].n", 2.0),
            (".b", "x"),
        ]

    @pytest.mark.parametrize("value", [None, [], {}, "", {"a": None}])
    def test_empty_values_have_no_leaves(self, value):
        assert leaves(value) == []


class TestFieldAgreement:
    def test_equal_scalars_after_normalizing_agree(self):
        assert field_agreement(" INV-1", "inv-1") == 1.0

    def test_different_scalars_disagree(self):
        assert field_agreement("INV-1", "INV-2") == 0.0

    def test_both_missing_agree(self):
        assert field_agreement(None, []) == 1.0

    def test_one_missing_disagrees(self):
        assert field_agreement(None, "x") == 0.0
        assert field_agreement("x", None) == 0.0

    def test_array_order_does_not_matter(self):
        assert field_agreement([{"n": 2}, {"n": 1}], [{"n": 1}, {"n": 2}]) == 1.0

    def test_partial_arrays_score_f1(self):
        # 2 of 3 found, nothing extra: precision 1, recall 2/3.
        score = field_agreement([{"n": 1}, {"n": 2}], [{"n": 1}, {"n": 2}, {"n": 3}])
        assert score == pytest.approx(0.8)


class TestScoreModel:
    def test_scores_each_field_over_the_documents_the_reference_answered(self):
        reference = _rows(
            {
                "@s/a.pdf": {"invoice": "A", "total": 1, "rows": [{"n": 1}]},
                "@s/b.pdf": {"invoice": "B", "total": 2, "rows": []},
                "@s/c.pdf": None,  # the reference failed: not scored
            }
        )
        rows = _rows(
            {
                "@s/a.pdf": {"invoice": "A", "total": 1, "rows": [{"n": 1}]},
                "@s/b.pdf": {"invoice": "X", "total": 2, "rows": []},
                "@s/c.pdf": {"invoice": "C", "total": 3, "rows": []},
            }
        )
        score = score_model("m", rows, reference, _FIELDS)
        assert score.documents == 2
        assert score.failed == 0
        assert score.field_scores == {"invoice": 0.5, "total": 1.0, "rows": 1.0}
        assert score.worst == 0.5
        assert score.input_tokens == 300 and score.output_tokens == 30

    def test_a_document_the_model_failed_scores_zero_on_every_field(self):
        reference = _rows({"@s/a.pdf": {"invoice": None, "total": None, "rows": []}})
        score = score_model("m", _rows({"@s/a.pdf": None}), reference, _FIELDS)
        assert score.failed == 1
        assert score.field_scores == {"invoice": 0.0, "total": 0.0, "rows": 0.0}

    def test_documents_an_image_model_was_not_sent_are_not_scored(self):
        reference = _rows(
            {
                "@s/a.pdf": {"invoice": "A", "total": 1, "rows": []},
                "@s/long.pdf": {"invoice": "L", "total": 2, "rows": []},
            }
        )
        rows = _rows({"@s/a.pdf": {"invoice": "A", "total": 1, "rows": []}})
        rows["@s/long.pdf"] = proc.result_row(_not_sent("@s/long.pdf"), _FIELDS)
        score = score_model("gemma", rows, reference, _FIELDS)
        assert score.documents == 1 and score.failed == 0
        assert score.worst == 1.0

    def test_the_reference_agrees_with_itself(self):
        reference = _rows({"@s/a.pdf": {"invoice": "A", "total": 1, "rows": []}})
        assert score_model("ref", reference, reference, _FIELDS).qualifies(1.0)


class TestSelectModel:
    def test_picks_the_cheapest_model_whose_every_field_meets_the_threshold(self):
        scores = [
            ModelScore("xs", {"a": 1.0, "b": 0.5}, documents=3),  # one field below
            ModelScore("s", {}, error="unknown model"),
            ModelScore("m", {"a": 0.95, "b": 0.9}, documents=3),
            ModelScore("l", {"a": 1.0, "b": 1.0}, documents=3),
        ]
        assert select_model(scores, 0.9).model == "m"

    def test_none_when_nothing_was_scored(self):
        assert select_model([ModelScore("ref", {}, documents=0)], 0.9) is None


# ---------------------------------------------------------------------------
# Sampling and budgets
# ---------------------------------------------------------------------------


class TestSampleSize:
    @pytest.mark.parametrize(
        "documents, expected",
        [(0, 0), (1, 0), (4, 0), (5, 1), (10, 2), (49, 9), (50, 10), (1000, 10)],
    )
    def test_ten_documents_or_a_fifth_whichever_is_fewer(self, documents, expected):
        assert sample_size(documents, _MODEL_SELECTION) == expected


class TestSampleFiles:
    def test_the_sample_spreads_over_the_documents_sorted_by_size(self):
        listed = [(f"@s/{i}.pdf", size) for i, size in enumerate([50, 10, 40, 20, 30])]
        listed += [(f"@s/x{i}.pdf", 100 + i) for i in range(5)]
        sample, rest = sample_files(listed, _MODEL_SELECTION)
        # 10 documents -> 2, the smallest and the largest.
        assert sample == [("@s/1.pdf", 10), ("@s/x4.pdf", 104)]
        assert set(sample) | set(rest) == set(listed)
        assert not set(sample) & set(rest)

    def test_unreadable_files_are_never_sampled(self):
        listed = [(f"@s/{i}.pdf", i) for i in range(5)]
        listed += [("@s/a.exe", 1), ("@s/big.pdf", 10**12)]
        sample, rest = sample_files(listed, _MODEL_SELECTION)
        assert sample == [("@s/2.pdf", 2)]
        assert ("@s/a.exe", 1) in rest and ("@s/big.pdf", 10**12) in rest

    def test_too_few_documents_means_no_sample(self):
        listed = [(f"@s/{i}.pdf", i) for i in range(4)]
        assert sample_files(listed, _MODEL_SELECTION) == ([], listed)


class TestBudgets:
    def test_a_capped_model_gets_its_ceiling_and_lower_targets(self):
        budgets = cap_budgets(_CALL_BUDGETS, 65536)
        assert budgets["range"]["max_tokens"] == 65536
        assert budgets["range"]["target"] == int(65536 * 0.6)
        assert budgets["merge_target"] == min(_CALL_BUDGETS["merge_target"], 39321)

    def test_the_rest_falls_back_to_the_read_models(self):
        assert read_models("gemini-3.8-flash", _EXTRACTION_MODELS) == [
            "gemini-3.8-flash",
            "claude-sonnet-5-5",
            "claude-opus-5",
        ]
        assert read_models("claude-opus-5", _EXTRACTION_MODELS) == [
            "claude-opus-5",
            "claude-sonnet-5-5",
        ]


# ---------------------------------------------------------------------------
# Per-field prompt optimization
# ---------------------------------------------------------------------------


class TestExcerpts:
    def test_pages_text_of_a_text_file_is_its_content(self):
        assert pages_text("@s/a.txt", b"hello") == ["hello"]

    def test_pages_text_of_other_types_is_empty(self):
        assert pages_text("@s/a.png", b"\x89PNG") == []

    def test_an_unreadable_pdf_has_no_text(self):
        assert pages_text("@s/a.pdf", b"not a pdf") == []

    def test_value_texts_cover_printed_number_formats(self):
        assert value_texts(1234.5) == ["1,234.50", "1234.50", "1234.5"]
        assert value_texts(1500) == ["1,500.00", "1500.00", "1,500", "1500"]
        assert value_texts("ab") == []
        assert value_texts(True) == []

    def test_finds_the_text_around_a_value_on_its_page(self):
        pages = ["nothing here", "Amount due:   $1,234.50\nThank you"]
        assert find_excerpts(pages, 1234.5) == [(2, "Amount due: $1,234.50 Thank you")]

    def test_matches_text_without_regard_to_case_or_whitespace(self):
        pages = ["Customer:  ACME\n  Corp, Ohio"]
        assert find_excerpts(pages, "acme corp") == [(1, "Customer: ACME Corp, Ohio")]

    def test_keeps_at_most_two_excerpts_per_value(self):
        pages = ["row aaa row bbb row ccc"]
        assert (
            len(find_excerpts(pages, [{"n": "aaa"}, {"n": "bbb"}, {"n": "ccc"}])) == 2
        )

    def test_nothing_when_the_value_is_not_printed(self):
        assert find_excerpts(["abc"], "xyz") == []

    def test_cuts_the_text_to_the_excerpt_width(self, monkeypatch):
        monkeypatch.setattr(proc, "EXCERPT_CHARS", 3)
        assert find_excerpts(["0123456789 INV-77 abcdefgh"], "INV-77") == [
            (1, "89 INV-77 ab")
        ]


class TestFieldOptimizerPrompt:
    def test_shows_each_document_with_both_values_and_their_text(self):
        request = field_optimizer_prompt(
            "total",
            {"type": "number"},
            "Extract.",
            [
                FieldExample(1234.5, 99, [(2, "Amount due: 1,234.50")], []),
                FieldExample(10, 1, [], [(1, "page 1 of 3")]),
            ],
        )
        assert "Field: total" in request
        assert 'Current JSON Schema of the field: {"type": "number"}' in request
        assert "Extraction instructions: Extract." in request
        assert (
            "Document 1:\n"
            '  Text around the reference\'s value: page 2: "Amount due: 1,234.50"\n'
            "  Reference's value: 1234.5\n"
            "  Model's value: 99\n"
            "  Text around the model's value: (not found in the document's text)"
        ) in request
        assert "Document 2:" in request

    @pytest.mark.parametrize(
        "text, expected",
        [
            ("  Be precise. ", "Be precise."),
            ('"Be \\"precise\\"."', 'Be "precise".'),
            ("", None),
            (None, None),
        ],
    )
    def test_unquotes_the_answer(self, text, expected):
        assert unquoted(text) == expected

    def test_with_descriptions_replaces_only_the_given_fields(self):
        revised = with_descriptions(_FORMAT, {"total": "The amount due.", "nope": "x"})
        assert revised["schema"]["properties"]["total"]["description"] == (
            "The amount due."
        )
        assert revised["schema"]["properties"]["invoice"]["description"] == (
            "The invoice number."
        )
        assert "nope" not in revised["schema"]["properties"]
        # The original is left as it was.
        assert "description" not in _SCHEMA["properties"]["total"]


# ---------------------------------------------------------------------------
# ImageExtraction
# ---------------------------------------------------------------------------


class TestImageExtraction:
    def _load(self, monkeypatch, name, content):
        monkeypatch.setattr(
            DocumentExtraction, "read_bytes", staticmethod(lambda path: content)
        )
        session = mock.MagicMock()
        extraction = ImageExtraction(
            session, _FORMAT, "Extract.", ["gemma-4-31b"], _CALL_BUDGETS, "@w", True
        )
        return session, extraction, extraction.load_file(name, 30, len(content))

    def test_a_single_page_pdf_is_read(self, monkeypatch):
        _, _, file = self._load(monkeypatch, "@s/one.pdf", _pdf(1))
        assert not file.failed and file.pages == 1

    @pytest.mark.parametrize(
        "name, content", [("@s/two.pdf", _pdf(2)), ("@s/a.txt", b"hello")]
    )
    def test_other_files_are_not_sent(self, monkeypatch, name, content):
        _, _, file = self._load(monkeypatch, name, content)
        assert file.failed and file.errors == [NOT_SENT_TO_IMAGE_MODEL_ERROR]

    def test_an_image_file_is_sent_as_it_is(self, monkeypatch):
        _, extraction, file = self._load(monkeypatch, "@s/scan.png", b"\x89PNG")
        assert not file.failed
        assert extraction.stage_part(file, 1, 1) == "@s/scan.png"

    def test_the_page_of_a_pdf_is_staged_as_a_png(self, monkeypatch):
        session, extraction, file = self._load(monkeypatch, "@s/one.pdf", _pdf(1))
        monkeypatch.setattr(proc, "render_page_png", lambda content: b"png-bytes")
        staged = extraction.stage_part(file, 1, 1)
        assert staged.startswith("@w/document_pages/") and staged.endswith(
            "/one_p1.png"
        )
        stream, target = session.file.put_stream.call_args[0]
        assert stream.getvalue() == b"png-bytes" and target == staged

    def test_render_page_png_draws_the_first_page(self):
        pytest.importorskip("pypdfium2")
        assert proc.render_page_png(_pdf(1), dpi=72)[:8] == b"\x89PNG\r\n\x1a\n"

    def test_model_selection_uses_it_for_image_models_only(self, monkeypatch):
        kinds = []

        def fake_extract_in_batches(self, listed, max_calls_per_file):
            kinds.append((type(self).__name__, self.models))
            return []

        monkeypatch.setattr(
            DocumentExtraction, "extract_in_batches", fake_extract_in_batches
        )
        selection = ModelSelection(
            mock.MagicMock(),
            _MODEL_SELECTION,
            _FORMAT,
            "Extract.",
            list(_EXTRACTION_MODELS),
            _CALL_BUDGETS,
            30,
            "@w",
            True,
        )
        selection.extract(["gemma-4-31b", "claude-sonnet-5-5"], [])
        selection.extract(["gemini-3.8-flash", "claude-sonnet-5-5"], [])
        assert kinds == [
            ("ImageExtraction", ["gemma-4-31b"]),
            ("DocumentExtraction", ["gemini-3.8-flash", "claude-sonnet-5-5"]),
        ]


# ---------------------------------------------------------------------------
# ModelSelection
# ---------------------------------------------------------------------------

_LISTED = [(f"@s/{i}.pdf", i) for i in range(10)]  # sample: @s/0.pdf and @s/9.pdf
_TRUTH = {name: {"invoice": name, "total": 1, "rows": []} for name, _ in _LISTED}
_CLOSE = {  # the sample's @s/9.pdf has the wrong total: total scores 0.5
    **_TRUTH,
    "@s/9.pdf": {"invoice": "@s/9.pdf", "total": 9, "rows": []},
}


class _FakeSelection(ModelSelection):
    """ModelSelection whose extraction answers from ``answers[model]`` (or raises it),
    and ``revised`` for calls with a revised schema; every call is recorded."""

    multi_page = frozenset()

    def __init__(self, answers, revised=None, config=None) -> None:
        super().__init__(
            mock.MagicMock(),
            config or _MODEL_SELECTION,
            _FORMAT,
            _DEFAULT_PROMPT,
            list(_EXTRACTION_MODELS),
            _CALL_BUDGETS,
            30,
            "@stage",
            True,
        )
        self.answers, self.revised = answers, revised
        self.calls = []

    def extract(self, models, listed, response_format=None):
        self.calls.append((models, [name for name, _ in listed], response_format))
        answers = self.answers.get(models[0], _TRUTH)
        if response_format is not None and response_format != _FORMAT:
            answers = self.revised
        if isinstance(answers, Exception):
            raise answers
        return [
            _not_sent(name, size)
            if models[0] in self.image_models and name in self.multi_page
            else _state(name, answers.get(name), size)
            for name, size in listed
        ]


def _answers(overrides=None):
    answers = {model: _TRUTH for model in _TIERS}
    answers["gemma-4-31b"] = RuntimeError("No model is available")
    answers.update(overrides or {})
    return answers


class TestChooseAndExtract:
    def test_reads_the_rest_with_the_cheapest_model_that_agrees(self):
        selection = _FakeSelection(_answers())
        files = selection.choose_and_extract(_LISTED)
        tiers = [call[0] for call in selection.calls[:4]]
        assert tiers == [[model] for model in _TIERS]
        assert all(call[1] == ["@s/0.pdf", "@s/9.pdf"] for call in selection.calls[:4])
        # gemini agrees everywhere: the rest is read with it, then the read's models.
        models, rest, response_format = selection.calls[4]
        assert models == ["gemini-3.8-flash", "claude-sonnet-5-5", "claude-opus-5"]
        assert rest == [f"@s/{i}.pdf" for i in range(1, 9)]
        assert response_format == _FORMAT
        assert len(selection.calls) == 5
        assert [file.path for file in files] == [name for name, _ in _LISTED]

    def test_falls_back_to_the_reference_when_no_cheaper_model_qualifies(self):
        answers = {model: _CLOSE for model in _TIERS}
        answers[_REFERENCE] = _TRUTH
        selection = _FakeSelection(answers)
        selection.choose_and_extract(_LISTED)
        assert selection.calls[-1][0] == ["claude-opus-5", "claude-sonnet-5-5"]

    def test_reads_everything_as_usual_when_the_reference_fails(self):
        selection = _FakeSelection(_answers({_REFERENCE: RuntimeError("boom")}))
        files = selection.choose_and_extract(_LISTED)
        models, listed, _ = selection.calls[-1]
        assert models == _EXTRACTION_MODELS
        assert listed == [name for name, _ in _LISTED]
        assert len(files) == len(_LISTED)

    def test_reads_everything_as_usual_without_a_sample(self):
        selection = _FakeSelection(_answers())
        selection.choose_and_extract(_LISTED[:4])
        assert [call[0] for call in selection.calls] == [_EXTRACTION_MODELS]

    def _optimized(
        self, monkeypatch, revised, answer="The amount due, not a subtotal."
    ):
        # gemini's total disagrees on one sample document (0.5); sonnet agrees everywhere,
        # so a threshold of 0.5 is needed for gemini to be chosen.
        config = {**_MODEL_SELECTION, "threshold": 0.5}
        selection = _FakeSelection(
            _answers({"gemini-3.8-flash": _CLOSE}), revised=revised, config=config
        )
        selection.session.sql.return_value.collect_nowait.return_value.result.return_value = [
            [answer]
        ]
        monkeypatch.setattr(
            ModelSelection,
            "document_pages",
            lambda self, path: [f"Invoice {path} Amount due: 1.00 Subtotal: 9.00"],
        )
        files = selection.choose_and_extract(_LISTED)
        return selection, files

    def test_asks_the_reference_for_each_disagreeing_field_with_document_text(
        self, monkeypatch
    ):
        selection, _ = self._optimized(monkeypatch, revised=_TRUTH)
        sql = selection.session.sql
        assert sql.call_count == 1  # only total disagreed
        query, kwargs = sql.call_args[0][0], sql.call_args[1]
        assert query == "SELECT AI_COMPLETE(?, ?)"
        model, request = kwargs["params"]
        assert model == _REFERENCE
        assert "Field: total" in request
        assert "Reference's value: 1\n" in request and "Model's value: 9\n" in request
        assert "Amount due: 1.00" in request and "Subtotal: 9.00" in request

    def test_keeps_descriptions_that_raise_agreement(self, monkeypatch):
        selection, files = self._optimized(monkeypatch, revised=_TRUTH)
        rerun_models, rerun_sample, rerun_format = selection.calls[4]
        assert rerun_models == ["gemini-3.8-flash"]
        assert rerun_sample == ["@s/0.pdf", "@s/9.pdf"]
        revised = rerun_format["schema"]["properties"]["total"]["description"]
        assert revised == "The amount due, not a subtotal."
        # The rest is read with the revised schema, and the sample keeps the rerun.
        assert selection.calls[5][2] == rerun_format
        by_path = {file.path: file for file in files}
        assert by_path["@s/9.pdf"].parts[0][1]["total"] == 1

    def test_drops_descriptions_that_do_not(self, monkeypatch):
        selection, files = self._optimized(monkeypatch, revised=_CLOSE)
        assert selection.calls[5][2] == _FORMAT
        by_path = {file.path: file for file in files}
        assert by_path["@s/9.pdf"].parts[0][1]["total"] == 9

    def test_no_rerun_when_the_optimizer_answers_nothing(self, monkeypatch):
        selection, _ = self._optimized(monkeypatch, revised=_TRUTH, answer="")
        assert len(selection.calls) == 5  # 4 tiers and the rest, no rerun


class TestImageModelChoice:
    def _run(self, multi_page):
        answers = {model: _TRUTH for model in _TIERS}  # gemma agrees everywhere
        selection = _FakeSelection(answers)
        selection.multi_page = frozenset(multi_page)
        files = selection.choose_and_extract(_LISTED)
        return selection, {file.path: file for file in files}

    def test_documents_gemma_cannot_take_go_to_the_cheapest_document_model(self):
        # @s/9.pdf is in the sample, @s/3.pdf and @s/5.pdf are in the rest.
        selection, files = self._run({"@s/9.pdf", "@s/3.pdf", "@s/5.pdf"})
        rest_models, rest, _ = selection.calls[4]
        assert rest_models == ["gemma-4-31b", "claude-sonnet-5-5", "claude-opus-5"]
        redo_models, redo, redo_format = selection.calls[5]
        assert redo_models == read_models("gemini-3.8-flash", _EXTRACTION_MODELS)
        assert redo == ["@s/3.pdf", "@s/5.pdf"]
        assert redo_format is None  # the user's schema
        assert len(selection.calls) == 6
        # Every file has an answer; none is left with the not-sent error.
        assert all(not file.failed for file in files.values())
        assert set(files) == {name for name, _ in _LISTED}

    def test_gemma_is_scored_only_on_the_documents_it_takes(self):
        # Both sample documents are multi-page: gemma scores nothing and is passed over.
        selection, _ = self._run({"@s/0.pdf", "@s/9.pdf"})
        assert selection.calls[4][0] == read_models(
            "gemini-3.8-flash", _EXTRACTION_MODELS
        )


class TestSummary:
    def test_lists_every_model_and_marks_the_chosen_one(self):
        scores = [
            ModelScore("a", error="unknown model"),
            ModelScore("b", {"x": 1.0}, documents=2, input_tokens=5, output_tokens=1),
        ]
        text = selection_summary(2, 10, _MODEL_SELECTION, scores, "b", None, False)
        assert "on 2 of 10 documents" in text
        assert "a: not scored: unknown model" in text
        assert "b: worst field 1.00" in text and text.endswith("<- selected")


class TestRunWithSelection:
    def test_run_hands_the_listed_files_to_model_selection(self, monkeypatch):
        listed = [("@s/a.pdf", 1)]
        monkeypatch.setattr(DocumentExtraction, "list_files", lambda self, path: listed)
        seen = {}

        def choose(self, files):
            seen["files"], seen["config"] = files, self.config
            return [_state("@s/a.pdf", {"invoice": "A", "total": 1, "rows": []})]

        monkeypatch.setattr(ModelSelection, "choose_and_extract", choose)
        session = mock.MagicMock()
        session.create_dataframe.side_effect = lambda rows, schema: rows
        rows = proc.run(
            session,
            "@s",
            json.dumps(_MODEL_SELECTION),
            json.dumps(_FORMAT),
            _DEFAULT_PROMPT,
            json.dumps(_EXTRACTION_MODELS),
            json.dumps(_CALL_BUDGETS),
            30,
            "@stage",
            True,
        )
        assert seen == {"files": listed, "config": _MODEL_SELECTION}
        assert rows[0][0] == "@s/a.pdf"


class TestSwitch:
    @pytest.mark.parametrize(
        "value, expected", [(True, True), (False, False), ("FALSE", False), ("x", True)]
    )
    def test_the_session_switch_turns_selection_off(self, value, expected):
        options = DocumentReaderOptions.from_reader_options(
            {"SCHEMA": _SCHEMA}, model_selection=value
        )
        assert options.model_selection is expected
