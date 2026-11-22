#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Model selection for documents() schema reads with the MODEL option set to "auto".

``extract_with_selected_model`` is the entry point:

1. List the documents and choose a sample (``sample_files``): at most
   ``_MAX_SAMPLE_DOCUMENTS``, or ``_MAX_SAMPLE_FRACTION`` of the documents if that is
   fewer, spread over the documents sorted by size.
2. Extract the sample with every model of ``_MODEL_TIERS`` (one extraction procedure
   CALL per model, run concurrently).
3. Score each model per field against the reference model's answers
   (``score_model``): the mean over the sample of an F1 over the field's values
   (``field_agreement``).
4. Choose the cheapest model whose every field agrees at least ``_AGREEMENT_THRESHOLD``
   (``select_model``). The reference is the last tier and agrees with itself, so it is
   chosen when no cheaper model is.
5. When the chosen model does not agree on every field, ask the reference model once for
   revised extraction instructions from the disagreements (``optimizer_prompt``), extract
   the sample again with them, and keep them only if the mean agreement rises while every
   field still meets the threshold.
6. Extract the rest of the documents with the chosen model and instructions; the sample's
   rows from step 2 (or 5) are returned beside them instead of being extracted again.

Steps 1 to 5 run when documents() is called, not when the DataFrame is collected: the
choice needs their answers. The outcome is logged (``ModelSelection.summary``). Nothing
here is cached, so each documents() call calibrates again.
"""

import json
import logging
import os
import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from snowflake.snowpark._internal.document_extract_proc import (
    MAX_FILE_READ_BYTES,
    SUPPORTED_FILE_EXTENSIONS,
)
from snowflake.snowpark._internal.document_reader import (
    _CALL_BUDGETS,
    _DEFAULT_EXTRACTION_PROMPT,
    _EXTRACTION_MODELS,
    _MAX_OUTPUT_TOKENS,
    call_extract_proc,
    proc_rows_schema,
    quote_stage_path,
    split_stage,
)
from snowflake.snowpark._internal.document_reader_options import DocumentReaderOptions

if TYPE_CHECKING:  # pragma: no cover
    from snowflake.snowpark.dataframe import DataFrame
    from snowflake.snowpark.session import Session

_logger = logging.getLogger(__name__)

# The model every other model is scored against.
_REFERENCE_MODEL = "claude-opus-5"
# Candidate models from the cheapest to the most expensive (T-shirt sizes XS, S, L, XL).
# The reference is the last one. A model AI_COMPLETE does not know for the account is
# skipped (its ModelScore carries the error).
_MODEL_TIERS = [
    "gemma-4-31b",
    "gemini-3.8-flash",
    "claude-sonnet-5-5",
    _REFERENCE_MODEL,
]
# Output ceilings below _MAX_OUTPUT_TOKENS: AI_COMPLETE rejects a larger max_tokens
# outright, so a model's calls are capped at its ceiling (``budgets_for``). A model not
# listed is sent _CALL_BUDGETS as they are.
_MODEL_MAX_OUTPUT_TOKENS = {"gemini-3.8-flash": 65536}
# A capped model's targets, which it is asked to stay under, are cut to this share of
# its ceiling.
_CAP_TARGET_FRACTION = 0.6

_MAX_SAMPLE_DOCUMENTS = 10
_MAX_SAMPLE_FRACTION = 0.2
# The agreement every field of a model must reach for the model to be chosen.
_AGREEMENT_THRESHOLD = 0.9

# Disagreements shown to the prompt optimizer, and the characters of each value shown.
_MAX_OPTIMIZER_EXAMPLES = 30
_MAX_OPTIMIZER_VALUE_CHARS = 1000

_OPTIMIZER_INSTRUCTION = (
    "You improve the instructions given to a document extraction model. The model reads "
    "each document and answers with JSON that conforms to the JSON Schema below. Its "
    "answers on sample documents were compared with those of a stronger reference model, "
    "and the fields listed below disagreed. Write revised extraction instructions that "
    "would make the model agree with the reference: general rules about how to find, "
    "interpret and format each field, never values copied from these documents. Keep "
    "every requirement of the current instructions. Answer with ONLY the revised "
    "instructions as plain text, at most 300 words."
)


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

_NUMBER_TEXT = re.compile(r"^[-+]?(\d{1,3}(,\d{3})+|\d+)(\.\d+)?$")


def normalize_scalar(value: Any) -> Any:
    """``value`` as compared between models: text with its whitespace collapsed and its
    case folded, a number (or text that is only a number, with optional thousands
    separators) as a float rounded to 6 places, a boolean as "true" or "false" (so it
    never equals 1.0). Empty text is None, the same as a missing value."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return round(float(value), 6)
    text = " ".join(str(value).split()).casefold()
    if not text:
        return None
    if _NUMBER_TEXT.match(text):
        return round(float(text.replace(",", "")), 6)
    return text


def leaves(value: Any, path: str = "") -> List[Tuple[str, Any]]:
    """Every scalar inside ``value`` as ``(path, normalized value)``. The path names the
    object keys on the way to it but not the array positions, so the order of an array's
    items does not matter. A missing value, an empty array or object, and empty text
    have no leaves."""
    if value is None:
        return []
    if isinstance(value, dict):
        return [leaf for key in value for leaf in leaves(value[key], f"{path}.{key}")]
    if isinstance(value, list):
        return [leaf for item in value for leaf in leaves(item, path + "[]")]
    normalized = normalize_scalar(value)
    return [] if normalized is None else [(path, normalized)]


def field_agreement(value: Any, reference: Any) -> float:
    """The F1 between the leaves of one field's ``value`` and of the ``reference``
    model's value for it, as multisets: 1.0 when they match (including both empty), 0.0
    when they share nothing. A scalar field scores 1.0 or 0.0. The leaves of an array of
    objects are matched without regard to which item they came from."""
    found, expected = Counter(leaves(value)), Counter(leaves(reference))
    if not found and not expected:
        return 1.0
    overlap = sum((found & expected).values())
    if not overlap:
        return 0.0
    precision = overlap / sum(found.values())
    recall = overlap / sum(expected.values())
    return 2 * precision * recall / (precision + recall)


@dataclass
class ModelScore:
    """How one model's answers on the sample agree with the reference's."""

    model: str
    # Per top-level schema field, the mean field_agreement over the documents scored.
    field_scores: Dict[str, float] = field(default_factory=dict)
    # Sample documents the reference answered; only those are scored.
    documents: int = 0
    # Of those, documents this model returned no answer for (each field scores 0).
    failed: int = 0
    # AI_COMPLETE's token usage over the whole sample, None when no call reported it.
    input_tokens: Optional[int] = None
    output_tokens: Optional[int] = None
    # Why the model could not be scored, such as AI_COMPLETE not knowing it.
    error: Optional[str] = None

    @property
    def worst(self) -> float:
        return min(self.field_scores.values(), default=0.0)

    @property
    def mean(self) -> float:
        if not self.field_scores:
            return 0.0
        return sum(self.field_scores.values()) / len(self.field_scores)

    def qualifies(self, threshold: float) -> bool:
        """Whether every field agrees at least ``threshold`` on a scored sample."""
        return (
            self.error is None
            and self.documents > 0
            and bool(self.field_scores)
            and self.worst >= threshold
        )


def answer(row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """The merged answer of one extraction procedure row, or None when it has none."""
    if row is None or row.get("VALUE") is None:
        return None
    try:
        value = json.loads(row["VALUE"])
    except (TypeError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _token_sum(rows: List[Dict[str, Any]], column: str) -> Optional[int]:
    counts = [row.get(column) for row in rows if row.get(column) is not None]
    return sum(int(count) for count in counts) if counts else None


def score_model(
    model: str,
    rows: Dict[str, Dict[str, Any]],
    reference_rows: Dict[str, Dict[str, Any]],
    fields: List[str],
) -> ModelScore:
    """``model``'s per-field agreement with the reference. ``rows`` and
    ``reference_rows`` are extraction procedure rows keyed by STAGE_PATH. A document the
    reference has no answer for is not scored; one ``model`` has no answer for scores 0
    on every field."""
    totals = {name: 0.0 for name in fields}
    documents = failed = 0
    for path, reference_row in reference_rows.items():
        expected = answer(reference_row)
        if expected is None:
            continue
        documents += 1
        found = answer(rows.get(path))
        if found is None:
            failed += 1
            continue
        for name in fields:
            totals[name] += field_agreement(found.get(name), expected.get(name))
    listed = list(rows.values())
    return ModelScore(
        model=model,
        field_scores=(
            {name: total / documents for name, total in totals.items()}
            if documents
            else {}
        ),
        documents=documents,
        failed=failed,
        input_tokens=_token_sum(listed, "INPUT_TOKENS"),
        output_tokens=_token_sum(listed, "OUTPUT_TOKENS"),
    )


def select_model(scores: List[ModelScore], threshold: float) -> Optional[ModelScore]:
    """The first (cheapest) of ``scores``, in tier order, that qualifies."""
    return next((score for score in scores if score.qualifies(threshold)), None)


# ---------------------------------------------------------------------------
# Sampling and budgets
# ---------------------------------------------------------------------------


def sample_size(documents: int) -> int:
    """How many of ``documents`` to calibrate on: _MAX_SAMPLE_DOCUMENTS or
    _MAX_SAMPLE_FRACTION of them, whichever is fewer, and at least one."""
    if documents <= 0:
        return 0
    return max(1, min(_MAX_SAMPLE_DOCUMENTS, int(documents * _MAX_SAMPLE_FRACTION)))


def is_sampleable(name: str, size: Optional[int]) -> bool:
    """Whether the extraction procedure would read the file at all: a supported type,
    not over its size limit."""
    extension = os.path.splitext(name)[1].lower()
    return extension in SUPPORTED_FILE_EXTENSIONS and (
        size is None or size <= MAX_FILE_READ_BYTES
    )


def sample_files(
    listed: List[Tuple[str, Optional[int]]],
) -> Tuple[List[Tuple[str, Optional[int]]], List[Tuple[str, Optional[int]]]]:
    """``listed`` split into the sample and the rest. The sample is ``sample_size`` of
    the files the procedure can read, evenly spaced over them sorted by size, so it
    covers short and long documents alike; files it cannot read are always in the rest
    (where they get their usual per-file error)."""
    candidates = sorted(
        (file for file in listed if is_sampleable(*file)),
        key=lambda file: (file[1] or 0, file[0]),
    )
    count = sample_size(len(candidates))
    if count == 0:
        return [], list(listed)
    if count == 1:
        picked = {candidates[len(candidates) // 2]}
    else:
        step = (len(candidates) - 1) / (count - 1)
        picked = {candidates[round(i * step)] for i in range(count)}
    sample = [file for file in candidates if file in picked]
    rest = [file for file in listed if file not in picked]
    return sample, rest


def budgets_for(model: str) -> Dict[str, Any]:
    """_CALL_BUDGETS capped at ``model``'s output ceiling (_MODEL_MAX_OUTPUT_TOKENS):
    every ``max_tokens`` at most the ceiling and every target at most
    _CAP_TARGET_FRACTION of it."""
    cap = _MODEL_MAX_OUTPUT_TOKENS.get(model)
    if not cap or cap >= _MAX_OUTPUT_TOKENS:
        return _CALL_BUDGETS
    target = int(cap * _CAP_TARGET_FRACTION)
    return {
        key: (
            {
                k: (min(v, cap) if k == "max_tokens" else min(v, target))
                for k, v in value.items()
            }
            if isinstance(value, dict)
            else min(value, target)
        )
        for key, value in _CALL_BUDGETS.items()
    }


def read_models(model: str) -> List[str]:
    """The models the final read may use: the chosen one, then the read's usual models
    should AI_COMPLETE stop knowing it. Their ceilings are at least the chosen model's,
    so its capped budgets suit them."""
    return [model] + [m for m in _EXTRACTION_MODELS if m != model]


# ---------------------------------------------------------------------------
# Prompt optimization
# ---------------------------------------------------------------------------


def _shown(value: Any) -> str:
    text = json.dumps(value, default=str)
    if len(text) > _MAX_OPTIMIZER_VALUE_CHARS:
        text = text[:_MAX_OPTIMIZER_VALUE_CHARS] + "...(cut)"
    return text


def optimizer_prompt(
    predicate: str,
    schema: Dict[str, Any],
    rows: Dict[str, Dict[str, Any]],
    reference_rows: Dict[str, Dict[str, Any]],
    fields: List[str],
) -> Optional[str]:
    """The request asking the reference model for revised instructions, listing where
    the chosen model's answers (``rows``) disagree with the reference's, or None when
    they disagree nowhere."""
    examples = []
    for number, (path, reference_row) in enumerate(sorted(reference_rows.items()), 1):
        expected = answer(reference_row)
        if expected is None:
            continue
        found = answer(rows.get(path)) or {}
        for name in fields:
            if field_agreement(found.get(name), expected.get(name)) < 1.0:
                examples.append(
                    f"- field {name!r}, document {number}: model answered "
                    f"{_shown(found.get(name))}; reference answered "
                    f"{_shown(expected.get(name))}"
                )
    if not examples:
        return None
    return (
        f"{_OPTIMIZER_INSTRUCTION}\n\nCurrent instructions:\n{predicate}\n\n"
        f"JSON Schema:\n{json.dumps(schema, separators=(',', ':'))}\n\n"
        "Disagreements:\n" + "\n".join(examples[:_MAX_OPTIMIZER_EXAMPLES])
    )


def _unquoted(text: Any) -> Optional[str]:
    """AI_COMPLETE's text answer, without the JSON string quotes it can come in."""
    if not isinstance(text, str):
        return None
    body = text.strip()
    if body.startswith('"') and body.endswith('"'):
        try:
            decoded = json.loads(body)
        except ValueError:
            decoded = None
        if isinstance(decoded, str):
            body = decoded.strip()
    return body or None


def revised_predicate(session: "Session", request: str) -> Optional[str]:
    """The reference model's revised instructions for ``request``, or None when the
    call fails or answers nothing."""
    try:
        rows = session.sql(
            "SELECT AI_COMPLETE(?, ?)", params=[_REFERENCE_MODEL, request]
        ).collect()
    except Exception as exc:
        _logger.warning("documents() prompt optimization call failed: %s", exc)
        return None
    return _unquoted(rows[0][0]) if rows else None


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


@dataclass
class ModelSelection:
    """What calibration found and chose."""

    sample: List[str]
    documents: int
    scores: List[ModelScore]
    model: str
    predicate: str
    # The chosen model's score with the revised instructions, when they were tried.
    revised: Optional[ModelScore] = None
    prompt_accepted: bool = False

    def summary(self) -> str:
        lines = [
            f"documents() model selection on {len(self.sample)} of {self.documents} "
            f"documents (reference {_REFERENCE_MODEL}, threshold "
            f"{_AGREEMENT_THRESHOLD:.2f} on every field):"
        ]
        for score in self.scores:
            mark = "  <- selected" if score.model == self.model else ""
            if score.error is not None:
                lines.append(f"  {score.model}: not scored: {score.error}{mark}")
                continue
            fields = ", ".join(f"{k}={v:.2f}" for k, v in score.field_scores.items())
            lines.append(
                f"  {score.model}: worst field {score.worst:.2f}, mean "
                f"{score.mean:.2f}, {score.failed} of {score.documents} failed, tokens "
                f"in/out {score.input_tokens}/{score.output_tokens} [{fields}]{mark}"
            )
        if self.revised is not None:
            outcome = "accepted" if self.prompt_accepted else "rejected"
            lines.append(
                f"  revised instructions: mean {self.revised.mean:.2f}, worst field "
                f"{self.revised.worst:.2f}: {outcome}"
            )
        return "\n".join(lines)


def list_stage_files(session: "Session", path: str) -> List[Tuple[str, Optional[int]]]:
    """Every file under ``path`` as ``(stage reference, listed size)``, referenced as
    the extraction procedure references them (``qualify_listed_name``)."""
    stage, listed_prefix = split_stage(path)
    files = []
    for row in session.sql(f"LIST {quote_stage_path(path)}").collect():
        try:
            size: Optional[int] = int(row["size"])
        except (KeyError, IndexError, TypeError, ValueError):
            size = None
        files.append((f"{stage}/{row['name'][listed_prefix:]}", size))
    return files


def _keyed(rows: List[Any]) -> Dict[str, Dict[str, Any]]:
    names = proc_rows_schema().names
    keyed = {}
    for row in rows:
        values = row.as_dict() if hasattr(row, "as_dict") else dict(row)
        keyed[values["STAGE_PATH"]] = {name: values.get(name) for name in names}
    return keyed


def extract_sample(
    session: "Session",
    path: str,
    sample: List[Tuple[str, Optional[int]]],
    options: DocumentReaderOptions,
    models: List[str],
    predicate: str,
) -> Dict[str, Any]:
    """Each of ``models``' procedure rows for ``sample``, keyed by STAGE_PATH, or the
    exception its CALL raised (such as AI_COMPLETE not knowing the model). The CALLs run
    concurrently."""
    jobs = {}
    for model in models:
        try:
            jobs[model] = call_extract_proc(
                session,
                path,
                sample,
                options,
                [model],
                predicate,
                budgets_for(model),
            ).collect_nowait()
        except Exception as exc:
            jobs[model] = exc
    results: Dict[str, Any] = {}
    for model, job in jobs.items():
        if isinstance(job, Exception):
            results[model] = job
            continue
        try:
            results[model] = _keyed(job.result())
        except Exception as exc:
            results[model] = exc
    return results


def _error_text(exc: Exception) -> str:
    return " ".join(str(exc).split())[:300]


def calibrate(
    session: "Session",
    path: str,
    sample: List[Tuple[str, Optional[int]]],
    documents: int,
    options: DocumentReaderOptions,
) -> Tuple[Optional[ModelSelection], Dict[str, Dict[str, Any]]]:
    """Steps 2 to 5: the selection, and the chosen setting's rows for the sample. None
    when the reference could not be scored against (it failed, or answered no sample
    document)."""
    fields = options.extraction.fields
    predicate = options.prompt or _DEFAULT_EXTRACTION_PROMPT
    results = extract_sample(session, path, sample, options, _MODEL_TIERS, predicate)
    reference_rows = results[_REFERENCE_MODEL]
    if isinstance(reference_rows, Exception):
        _logger.warning(
            "documents() model selection skipped: the reference model %s failed: %s",
            _REFERENCE_MODEL,
            _error_text(reference_rows),
        )
        return None, {}
    scores = []
    for model in _MODEL_TIERS:
        rows = results[model]
        if isinstance(rows, Exception):
            scores.append(ModelScore(model=model, error=_error_text(rows)))
        else:
            scores.append(score_model(model, rows, reference_rows, fields))
    chosen = select_model(scores, _AGREEMENT_THRESHOLD)
    if chosen is None:
        _logger.warning(
            "documents() model selection skipped: the reference model %s answered "
            "none of the %d sample documents",
            _REFERENCE_MODEL,
            len(sample),
        )
        return None, {}
    selection = ModelSelection(
        sample=[name for name, _ in sample],
        documents=documents,
        scores=scores,
        model=chosen.model,
        predicate=predicate,
    )
    chosen_rows = results[chosen.model]
    if chosen.model == _REFERENCE_MODEL or chosen.mean >= 1.0:
        return selection, chosen_rows

    request = optimizer_prompt(
        predicate,
        options.extraction.ai_complete_format["schema"],
        chosen_rows,
        reference_rows,
        fields,
    )
    revised = revised_predicate(session, request) if request else None
    if revised is None:
        return selection, chosen_rows
    rerun = extract_sample(session, path, sample, options, [chosen.model], revised)[
        chosen.model
    ]
    if isinstance(rerun, Exception):
        _logger.warning(
            "documents() revised instructions not tried: %s", _error_text(rerun)
        )
        return selection, chosen_rows
    selection.revised = score_model(chosen.model, rerun, reference_rows, fields)
    if (
        selection.revised.qualifies(_AGREEMENT_THRESHOLD)
        and selection.revised.mean > chosen.mean
    ):
        selection.prompt_accepted = True
        selection.predicate = revised
        return selection, rerun
    return selection, chosen_rows


def extract_with_selected_model(
    session: "Session", path: str, options: DocumentReaderOptions
) -> "DataFrame":
    """The extraction procedure's rows for every file under ``path``, read with the model
    (and instructions) calibration chose; see the module docstring. When there is
    nothing to calibrate on, or the reference could not be scored against, the read
    goes ahead with its usual models."""
    listed = list_stage_files(session, path)
    sample, rest = sample_files(listed)
    selection = None
    if sample:
        selection, sample_rows = calibrate(session, path, sample, len(listed), options)
    if selection is None:
        return call_extract_proc(
            session,
            path,
            None,
            options,
            _EXTRACTION_MODELS,
            options.prompt or _DEFAULT_EXTRACTION_PROMPT,
            _CALL_BUDGETS,
        )
    _logger.info(selection.summary())
    schema = proc_rows_schema()
    sampled = session.create_dataframe(
        [[row[name] for name in schema.names] for row in sample_rows.values()],
        schema=schema,
    )
    if not rest:
        return sampled
    return call_extract_proc(
        session,
        path,
        rest,
        options,
        read_models(selection.model),
        selection.predicate,
        budgets_for(selection.model),
    ).union_all(sampled)
