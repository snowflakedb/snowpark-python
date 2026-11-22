#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import json
from unittest import mock

import pytest

from snowflake.snowpark._internal import document_model_selection as selection
from snowflake.snowpark._internal.document_model_selection import (
    ModelScore,
    _CALL_BUDGETS,
    _MODEL_TIERS,
    _REFERENCE_MODEL,
    budgets_for,
    calibrate,
    extract_with_selected_model,
    field_agreement,
    leaves,
    normalize_scalar,
    optimizer_prompt,
    read_models,
    sample_files,
    sample_size,
    score_model,
    select_model,
    _unquoted,
)
from snowflake.snowpark._internal.document_reader_options import (
    DocumentReaderOptions,
    ExtractionSpec,
)
from snowflake.snowpark.exceptions import SnowparkDataframeReaderException

_SCHEMA = {
    "type": "object",
    "properties": {
        "invoice": {"type": "string"},
        "total": {"type": "number"},
        "rows": {
            "type": "array",
            "items": {"type": "object", "properties": {"n": {"type": "integer"}}},
        },
    },
}
_FIELDS = ["invoice", "total", "rows"]


def _row(path, value, input_tokens=100, output_tokens=10):
    return {
        "STAGE_PATH": path,
        "VALUE": None if value is None else json.dumps(value),
        "ERROR": None if value is not None else "failed",
        "SCORES": None,
        "INPUT_TOKENS": input_tokens,
        "OUTPUT_TOKENS": output_tokens,
    }


def _rows(values):
    return {path: _row(path, value) for path, value in values.items()}


def _options(**kwargs):
    return DocumentReaderOptions(
        parse_mode="none",
        extraction=ExtractionSpec.from_response_format(_SCHEMA),
        auto_model=True,
        **kwargs,
    )


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
        [(0, 0), (1, 1), (4, 1), (10, 2), (49, 9), (50, 10), (1000, 10)],
    )
    def test_ten_documents_or_a_fifth_whichever_is_fewer(self, documents, expected):
        assert sample_size(documents) == expected


class TestSampleFiles:
    def test_the_sample_spreads_over_the_documents_sorted_by_size(self):
        listed = [(f"@s/{i}.pdf", size) for i, size in enumerate([50, 10, 40, 20, 30])]
        listed += [(f"@s/x{i}.pdf", 100 + i) for i in range(5)]
        sample, rest = sample_files(listed)
        # 10 documents -> 2, the smallest and the largest.
        assert sample == [("@s/1.pdf", 10), ("@s/x4.pdf", 104)]
        assert set(sample) | set(rest) == set(listed)
        assert not set(sample) & set(rest)

    def test_unreadable_files_are_never_sampled(self):
        listed = [("@s/a.exe", 1), ("@s/b.pdf", 2), ("@s/big.pdf", 10**12)]
        sample, rest = sample_files(listed)
        assert sample == [("@s/b.pdf", 2)]
        assert rest == [("@s/a.exe", 1), ("@s/big.pdf", 10**12)]

    def test_nothing_readable_means_no_sample(self):
        assert sample_files([("@s/a.exe", 1)]) == ([], [("@s/a.exe", 1)])


class TestBudgets:
    def test_a_model_without_a_lower_ceiling_gets_the_budgets_as_they_are(self):
        assert budgets_for("claude-sonnet-5-5") is _CALL_BUDGETS

    def test_a_capped_model_gets_its_ceiling_and_lower_targets(self):
        budgets = budgets_for("gemini-3.8-flash")
        assert budgets["range"]["max_tokens"] == 65536
        assert budgets["range"]["target"] == int(65536 * 0.6)
        assert budgets["merge_target"] == min(_CALL_BUDGETS["merge_target"], 39321)

    def test_the_final_read_falls_back_to_the_usual_models(self):
        assert read_models("gemini-3.8-flash") == [
            "gemini-3.8-flash",
            "claude-sonnet-5-5",
            "claude-opus-5",
        ]
        assert read_models("claude-opus-5") == ["claude-opus-5", "claude-sonnet-5-5"]


# ---------------------------------------------------------------------------
# Prompt optimization
# ---------------------------------------------------------------------------


class TestOptimizerPrompt:
    def test_lists_only_the_disagreeing_fields(self):
        reference = _rows({"@s/a.pdf": {"invoice": "A", "total": 1, "rows": []}})
        rows = _rows({"@s/a.pdf": {"invoice": "A", "total": 2, "rows": []}})
        request = optimizer_prompt("Extract.", _SCHEMA, rows, reference, _FIELDS)
        assert "Current instructions:\nExtract." in request
        assert "field 'total', document 1: model answered 2; reference answered 1" in (
            request
        )
        assert "field 'invoice'" not in request

    def test_none_when_everything_agrees(self):
        reference = _rows({"@s/a.pdf": {"invoice": "A", "total": 1, "rows": []}})
        assert optimizer_prompt("Extract.", _SCHEMA, reference, reference, _FIELDS) is (
            None
        )

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
        assert _unquoted(text) == expected


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

_SAMPLE = [("@s/a.pdf", 1), ("@s/b.pdf", 2)]
_TRUTH = {
    "@s/a.pdf": {"invoice": "A", "total": 1, "rows": [{"n": 1}]},
    "@s/b.pdf": {"invoice": "B", "total": 2, "rows": [{"n": 2}]},
}
_CLOSE = {  # agrees on every field but one document's total: total 0.5
    "@s/a.pdf": {"invoice": "A", "total": 1, "rows": [{"n": 1}]},
    "@s/b.pdf": {"invoice": "B", "total": 9, "rows": [{"n": 2}]},
}


def _patched_extract_sample(per_model, rerun=None):
    """extract_sample answering ``per_model[model]`` (rows, or an exception), and
    ``rerun`` for a call with instructions other than the default."""

    def fake(session, path, sample, options, models, predicate):
        if predicate != "Extract the requested fields from this document.":
            return {models[0]: _rows(rerun)}
        return {
            model: (
                per_model[model]
                if isinstance(per_model[model], Exception)
                else _rows(per_model[model])
            )
            for model in models
        }

    return mock.patch.object(selection, "extract_sample", side_effect=fake)


class TestCalibrate:
    def test_chooses_the_cheapest_model_that_agrees_everywhere(self):
        per_model = {
            "gemma-4-31b": RuntimeError("No model is available"),
            "gemini-3.8-flash": _CLOSE,
            "claude-sonnet-5-5": _TRUTH,
            _REFERENCE_MODEL: _TRUTH,
        }
        with _patched_extract_sample(per_model):
            chosen, rows = calibrate(mock.Mock(), "@s", _SAMPLE, 10, _options())
        assert chosen.model == "claude-sonnet-5-5"
        assert [s.model for s in chosen.scores] == _MODEL_TIERS
        assert chosen.scores[0].error == "No model is available"
        assert chosen.revised is None
        assert set(rows) == {"@s/a.pdf", "@s/b.pdf"}

    def test_falls_back_to_the_reference_when_no_cheaper_model_qualifies(self):
        per_model = {model: _CLOSE for model in _MODEL_TIERS}
        per_model[_REFERENCE_MODEL] = _TRUTH
        with _patched_extract_sample(per_model):
            chosen, _ = calibrate(mock.Mock(), "@s", _SAMPLE, 10, _options())
        assert chosen.model == _REFERENCE_MODEL

    def test_none_when_the_reference_fails(self):
        per_model = {model: _TRUTH for model in _MODEL_TIERS}
        per_model[_REFERENCE_MODEL] = RuntimeError("boom")
        with _patched_extract_sample(per_model):
            assert calibrate(mock.Mock(), "@s", _SAMPLE, 10, _options()) == (None, {})

    def _imperfect(self, monkeypatch, rerun, revised="Be precise."):
        # gemini qualifies at the threshold but is not perfect: rows of 1 document of
        # 10 disagree on total.
        sample = [(f"@s/{i}.pdf", i) for i in range(10)]
        truth = {name: {"invoice": name, "total": 1, "rows": []} for name, _ in sample}
        close = dict(truth)
        close["@s/0.pdf"] = {"invoice": "@s/0.pdf", "total": 2, "rows": []}
        per_model = {model: truth for model in _MODEL_TIERS}
        per_model["gemma-4-31b"] = RuntimeError("unknown")
        per_model["gemini-3.8-flash"] = close
        monkeypatch.setattr(selection, "revised_predicate", lambda s, r: revised)
        with _patched_extract_sample(per_model, rerun=rerun(truth, close)):
            return calibrate(mock.Mock(), "@s", sample, 50, _options())

    def test_keeps_revised_instructions_that_raise_agreement(self, monkeypatch):
        chosen, rows = self._imperfect(monkeypatch, lambda truth, close: truth)
        assert chosen.model == "gemini-3.8-flash"
        assert chosen.prompt_accepted
        assert chosen.predicate == "Be precise."
        assert chosen.revised.mean == 1.0
        assert json.loads(rows["@s/0.pdf"]["VALUE"])["total"] == 1

    def test_drops_revised_instructions_that_do_not(self, monkeypatch):
        chosen, rows = self._imperfect(monkeypatch, lambda truth, close: close)
        assert not chosen.prompt_accepted
        assert chosen.predicate == "Extract the requested fields from this document."
        assert json.loads(rows["@s/0.pdf"]["VALUE"])["total"] == 2

    def test_no_rerun_when_the_optimizer_answers_nothing(self, monkeypatch):
        chosen, _ = self._imperfect(monkeypatch, lambda t, c: t, revised=None)
        assert chosen.revised is None and not chosen.prompt_accepted


class TestExtractWithSelectedModel:
    def _run(self, listed, chosen):
        session = mock.MagicMock()
        call = mock.MagicMock()
        with mock.patch.object(
            selection, "list_stage_files", return_value=listed
        ), mock.patch.object(
            selection, "calibrate", return_value=chosen
        ), mock.patch.object(
            selection, "call_extract_proc", return_value=call
        ) as call_proc:
            result = extract_with_selected_model(session, "@s", _options())
        return session, call, call_proc, result

    def test_reads_the_rest_with_the_chosen_model_beside_the_sample_rows(self):
        listed = [(f"@s/{i}.pdf", i) for i in range(5)]
        chosen = selection.ModelSelection(
            sample=["@s/2.pdf"],
            documents=5,
            scores=[],
            model="gemini-3.8-flash",
            predicate="Be precise.",
        )
        rows = _rows({"@s/2.pdf": {"invoice": "A", "total": 1, "rows": []}})
        session, call, call_proc, result = self._run(listed, (chosen, rows))
        args = call_proc.call_args[0]
        assert args[2] == [f for f in listed if f[0] != "@s/2.pdf"]
        assert args[4] == read_models("gemini-3.8-flash")
        assert args[5] == "Be precise."
        assert args[6] == budgets_for("gemini-3.8-flash")
        sampled_rows = session.create_dataframe.call_args[0][0]
        assert [r[0] for r in sampled_rows] == ["@s/2.pdf"]
        call.union_all.assert_called_once_with(session.create_dataframe.return_value)
        assert result is call.union_all.return_value

    def test_reads_as_usual_when_calibration_gives_no_choice(self):
        _, _, call_proc, result = self._run([("@s/0.pdf", 1)], (None, {}))
        args = call_proc.call_args[0]
        assert args[2] is None
        assert args[4] == ["claude-sonnet-5-5", "claude-opus-5"]
        assert result is call_proc.return_value


# ---------------------------------------------------------------------------
# The MODEL option
# ---------------------------------------------------------------------------


class TestModelOption:
    def test_auto_turns_on_selection_for_a_schema_read(self):
        options = DocumentReaderOptions.from_reader_options(
            {"SCHEMA": _SCHEMA, "MODEL": "AUTO"}
        )
        assert options.auto_model

    def test_off_by_default(self):
        assert not DocumentReaderOptions.from_reader_options(
            {"SCHEMA": _SCHEMA}
        ).auto_model

    def test_any_other_value_is_refused(self):
        with pytest.raises(SnowparkDataframeReaderException, match="Valid values are"):
            DocumentReaderOptions.from_reader_options(
                {"SCHEMA": _SCHEMA, "MODEL": "claude-opus-5"}
            )

    def test_auto_needs_a_schema(self):
        with pytest.raises(SnowparkDataframeReaderException, match="'schema'"):
            DocumentReaderOptions.from_reader_options(
                {"PROMPT": "Summarize.", "MODEL": "auto"}
            )
