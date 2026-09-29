#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

#
# Copyright (c) 2012-2026 Snowflake Computing Inc. All rights reserved.
#
"""JSON Schema subset Cortex document AI functions accept.

Used by the document-reader planner so engine choice is based on schema
*shape* (scalars, string arrays, EXTRACT table objects), not on a
particular benchmark or field name.
"""

from __future__ import annotations

import copy
import re
from typing import Any

_DROP = frozenset(
    {
        "additionalProperties",
        "unevaluatedProperties",
        "unevaluatedItems",
        "default",
        "$schema",
        "$id",
        "$comment",
        "examples",
        "readOnly",
        "writeOnly",
        "deprecated",
        "minLength",
        "maxLength",
        "minItems",
        "maxItems",
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "pattern",
        "format",
        "const",
        "if",
        "then",
        "else",
        "dependentSchemas",
        "dependentRequired",
        "prefixItems",
    }
)

_EXTRACT_SCALARS = frozenset({"string", "number", "integer", "boolean"})

# BPA fallback when AI_EXTRACT cannot take the schema. Explicit .option("model")
# always wins; generative / research tasks keep the stronger default in
# document_reader.py.
DEFAULT_BPA_COMPLETE_MODEL = "claude-haiku-4-5"


def schema_properties(schema: Any) -> dict[str, Any]:
    if not isinstance(schema, dict):
        return {}
    if isinstance(schema.get("properties"), dict):
        return schema["properties"]
    inner = schema.get("schema")
    if isinstance(inner, dict) and isinstance(inner.get("properties"), dict):
        return inner["properties"]
    return {}


def is_table_property(defn: Any) -> bool:
    if not isinstance(defn, dict) or defn.get("type") != "object":
        return False
    nested = defn.get("properties") or {}
    if not nested:
        return False
    return all(
        isinstance(col_def, dict) and col_def.get("type") == "array"
        for col_def in nested.values()
    )


# Private bookkeeping key. Set while a union is being collapsed, consumed by
# _restore_nullability, and never present in the schema handed to Cortex.
_NULLABLE = "__nullable__"


def _restore_nullability(node: Any) -> Any:
    """Turn ``_NULLABLE`` markers back into an explicit nullable type.

    Runs last, so stringification has already settled each leaf's type. ``null``
    must be spelled out or Cortex JSON mode rejects the whole response when the
    model reports a genuinely blank field -- one empty optional field on an
    invoice otherwise discards the entire extraction.

    Spelled as ``type: [X, "null"]`` rather than ``anyOf: [{X}, {null}]``
    deliberately. Cortex's *input* schema validator requires a ``type`` key on
    every property: a bare ``anyOf`` is rejected with "please specify a valid
    json schema object ('type' missing?...)". claude-haiku-4-5 happens to
    tolerate the anyOf form, claude-4-sonnet does not -- so the union spelling
    would leave the payload valid only on one model.
    """
    if isinstance(node, list):
        return [_restore_nullability(item) for item in node]
    if not isinstance(node, dict):
        return node
    out = {
        key: _restore_nullability(value)
        for key, value in node.items()
        if key != _NULLABLE
    }
    if not node.get(_NULLABLE):
        return out
    declared = out.get("type")
    if isinstance(declared, list):
        if "null" not in declared:
            out["type"] = [*declared, "null"]
    elif isinstance(declared, str):
        out["type"] = [declared, "null"]
    else:
        # No resolved type to widen (e.g. an unresolvable ref). Leave it alone
        # rather than emit a property with no type at all.
        return out
    return out


def simplify_json_schema(schema: Any, *, scalars_as_string: bool = False) -> Any:
    if not isinstance(schema, dict):
        return schema
    root = copy.deepcopy(schema)
    defs = dict(root.get("$defs") or root.get("definitions") or {})
    simplified = _simplify(root, defs)
    if isinstance(simplified, dict):
        simplified.pop("$defs", None)
        simplified.pop("definitions", None)
        if "type" not in simplified and "properties" in simplified:
            simplified["type"] = "object"
        if scalars_as_string:
            simplified = _scalars_as_string(simplified)
    return _restore_nullability(simplified)


# Whether to send number/integer/boolean/array leaves to AI_COMPLETE as `string`.
#
# FALSE is the measured-correct default. Sending real types was worth +0.0217 on the
# 252-document short split and unblocked 32 documents that previously returned NOTHING
# The stringified form fails two ways:
#
#   1. Modern models return real JSON types, and json mode then discards the ENTIRE
#      response: `/state_local_row_1/state_income_tax 1511.82 type should be one of:
#      string,null, got number`. One dollar amount loses every other field.
#   2. `_scalars_as_string` DELETES an array's `items` and moves the item schema into the
#      description as prose, so the array is no longer declared an array. Nested arrays
#      then arrive at the caller as escaped JSON strings -- `census_statab` scored 0.0563
#      delivered versus 1.0000 produced, and its `tables` came back as 7,785 characters of
#      escaped JSON instead of a list of 2.
#
# ⚠️ Why the stringified form existed was never established -- the original docstring
# recorded what it did, not why. If some model or Cortex version once rejected real types,
# the guard was right then. This flag keeps that path reachable: set it True to restore the
# old behaviour if a customer schema regresses.
STRINGIFY_SCALARS_FOR_COMPLETE = False

# Whether to forward the caller's `required` list to AI_COMPLETE.
#
# FALSE because Cortex enforces `required` as MUST-BE-NON-NULL, not "key present". A single
# field the model cannot find on the page discards the whole extraction with
# `root level "invoice_number" value is required`. Stripping it recovered 6 of 6 invoices;
# `grafton_isotrope_invoice_19503` went from `value: null` to a full extraction with bank
# details, addresses and line items.
#
# ⚠️ This is the one fix with a real trade-off: it converts a LOUD total failure into a
# SILENT partial extraction. A caller who relied on `required` to guarantee a field is
# present no longer gets that guarantee from Cortex and must check for null themselves.
# The better long-term fix is to mark required fields nullable, or to require only fields
# genuinely always present -- neither is verified against live Cortex yet.
FORWARD_REQUIRED_TO_COMPLETE = False


def _strip_required(node: Any) -> Any:
    """Remove every `required` list, at any depth."""
    if isinstance(node, list):
        return [_strip_required(item) for item in node]
    if not isinstance(node, dict):
        return node
    return {
        key: _strip_required(value) for key, value in node.items() if key != "required"
    }


def for_cortex_complete(
    schema: Any,
    *,
    stringify_scalars: bool | None = None,
    forward_required: bool | None = None,
) -> Any:
    """Schema Cortex JSON mode accepts.

    Defaults send the caller's real types and drop `required`; see
    STRINGIFY_SCALARS_FOR_COMPLETE and FORWARD_REQUIRED_TO_COMPLETE for why, and pass
    the keywords explicitly to override per call.
    """
    if stringify_scalars is None:
        stringify_scalars = STRINGIFY_SCALARS_FOR_COMPLETE
    if forward_required is None:
        forward_required = FORWARD_REQUIRED_TO_COMPLETE
    prepared = simplify_json_schema(schema, scalars_as_string=stringify_scalars)
    if not forward_required:
        prepared = _strip_required(prepared)
    return prepared


PRESERVE_NUMERIC_DIGITS = "Preserve every printed digit; do not round or truncate."


def for_cortex_extract(schema: Any) -> Any:
    """Keep the caller's schema; stringify number/boolean/integer leaves only.

    Live AI_EXTRACT accepts leftover ``$ref`` / ``anyOf``. Inlining those into
    explicit arrays of objects is what Cortex rejects (``items ... must accept
    type 'string'``). Do not ``simplify_json_schema`` here.
    """
    annotated = annotate_numeric_leaves(schema)
    return _stringify_number_bool_types(annotated)


def annotate_numeric_leaves(schema: Any) -> Any:
    """Pre-process numeric leaves: ask the model to keep every printed digit.

    Walks ``properties``, ``items``, ``$defs``, and unions. Field names are
    not used. Idempotent. Mutates a copy.
    """
    if not isinstance(schema, dict):
        return schema
    root = copy.deepcopy(schema)
    _annotate_numeric_node(root)
    return root


def _annotate_numeric_node(node: Any) -> None:
    if isinstance(node, list):
        for item in node:
            _annotate_numeric_node(item)
        return
    if not isinstance(node, dict):
        return
    if _defn_is_numeric(node):
        desc = str(node.get("description") or "").strip()
        if PRESERVE_NUMERIC_DIGITS.lower() not in desc.lower():
            node["description"] = (
                f"{desc} {PRESERVE_NUMERIC_DIGITS}".strip()
                if desc
                else PRESERVE_NUMERIC_DIGITS
            )
    for key in ("properties", "$defs", "definitions"):
        nested = node.get(key)
        if isinstance(nested, dict):
            for child in nested.values():
                _annotate_numeric_node(child)
    for key in ("items", "additionalProperties"):
        if key in node:
            _annotate_numeric_node(node[key])
    for key in ("anyOf", "oneOf", "allOf"):
        if isinstance(node.get(key), list):
            _annotate_numeric_node(node[key])


def _defn_is_numeric(defn: dict[str, Any]) -> bool:
    json_type = defn.get("type")
    if isinstance(json_type, list):
        return any(item in {"number", "integer"} for item in json_type)
    if json_type in {"number", "integer"}:
        return True
    for key in ("anyOf", "oneOf"):
        options = defn.get(key)
        if isinstance(options, list) and any(
            isinstance(option, dict) and _defn_is_numeric(option) for option in options
        ):
            return True
    return False


_BOOL_TRUE = frozenset({"true", "yes", "y", "t", "1", "x", "checked"})
_BOOL_FALSE = frozenset({"false", "no", "n", "f", "0", "unchecked"})


def coerce_extracted(value: Any, schema: Any = None) -> Any:
    """Restore the JSON scalar type the schema declares. Two rules, nothing else.

    ``number``/``integer`` leaves: parse presentation text (``"$21,693"``,
    ``"(1,200)"``, ``"33.5%"``) back into a real JSON number.
    ``boolean`` leaves: ``Yes``/``No``/``X``/``checked`` become real bools.
    Field names are never used to infer types -- only the declared schema.

    Both rules exist because ``for_cortex_complete`` stringifies numeric and
    boolean leaves before sending the schema to Cortex, so the model is
    *required* to answer in strings. The official metric compares JSON types
    exactly (verified: gold ``3500`` vs ``"3500"`` scores 0.0), so without
    this the pipeline would be penalised for a transformation it performed
    itself. This function undoes our own stringification and must do nothing
    more.

    Every rule here is therefore a *type* restoration, never a change to
    which value the model reported. Seven other rules were deleted for
    failing that test or for measuring zero: placeholder-to-null, a
    street/city comma splitter (which turned ``PO BOX 1234 AUSTIN, TX 78701``
    into ``PO Box, 1234 AUSTIN, TX 78701`` -- 28 documents worse, 0 better),
    date-to-ISO, coded-entry string parsing, empty-string-to-null,
    ``"false"``/``"true"`` on non-boolean leaves, JSON re-parsing, and float
    rounding.
    """
    root_schema = schema if isinstance(schema, dict) else {}
    return _coerce_extracted(value, schema, root_schema)


def _coerce_extracted(value: Any, schema: Any, root_schema: dict[str, Any]) -> Any:
    if isinstance(value, str):
        stripped = value.strip()
        if _boolean_schema_type(schema, root_schema):
            parsed_bool = _parse_bool_token(stripped)
            if parsed_bool is not None:
                return parsed_bool
        numeric_type = _numeric_schema_type(schema, root_schema)
        if numeric_type is not None:
            parsed = _parse_numeric_string(stripped, integer=numeric_type == "integer")
            if parsed is not None:
                return parsed
        return value
    if isinstance(value, dict):
        resolved = _resolve_schema_node(schema, root_schema)
        properties = (
            resolved.get("properties")
            if isinstance(resolved, dict)
            and isinstance(resolved.get("properties"), dict)
            else {}
        )
        return {
            key: _coerce_extracted(child, properties.get(key), root_schema)
            for key, child in value.items()
        }
    if isinstance(value, list):
        resolved = _resolve_schema_node(schema, root_schema)
        items = resolved.get("items") if isinstance(resolved, dict) else None
        return [_coerce_extracted(item, items, root_schema) for item in value]
    return value


def _parse_bool_token(value: str) -> bool | None:
    token = value.strip().lower()
    if token in _BOOL_TRUE:
        return True
    if token in _BOOL_FALSE:
        return False
    return None


def _resolve_schema_node(schema: Any, root_schema: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(schema, dict):
        return {}
    if isinstance(schema.get("$ref"), str):
        ref = schema["$ref"]
        if ref.startswith("#/$defs/"):
            resolved = (root_schema.get("$defs") or {}).get(ref.split("/", 2)[-1])
        elif ref.startswith("#/definitions/"):
            resolved = (root_schema.get("definitions") or {}).get(ref.split("/", 2)[-1])
        else:
            resolved = None
        if isinstance(resolved, dict):
            siblings = {key: val for key, val in schema.items() if key != "$ref"}
            return {**resolved, **siblings}
    for keyword in ("anyOf", "oneOf"):
        options = schema.get(keyword)
        if isinstance(options, list):
            non_null = [
                option
                for option in options
                if isinstance(option, dict) and option.get("type") != "null"
            ]
            if len(non_null) == 1:
                # title / description / format sit on the union, not the branch.
                siblings = {
                    key: val
                    for key, val in schema.items()
                    if key not in {"anyOf", "oneOf", "allOf"}
                }
                return {**siblings, **_resolve_schema_node(non_null[0], root_schema)}
    return schema


def _numeric_schema_type(schema: Any, root_schema: dict[str, Any]) -> str | None:
    resolved = _resolve_schema_node(schema, root_schema)
    json_type = resolved.get("type")
    if isinstance(json_type, list):
        numeric = [item for item in json_type if item in {"number", "integer"}]
        non_null = [item for item in json_type if item != "null"]
        if len(numeric) == 1 and len(non_null) == 1:
            return numeric[0]
    elif json_type in {"number", "integer"}:
        return json_type
    return None


def _boolean_schema_type(schema: Any, root_schema: dict[str, Any]) -> bool:
    resolved = _resolve_schema_node(schema, root_schema)
    json_type = resolved.get("type")
    if json_type == "boolean":
        return True
    if isinstance(json_type, list):
        non_null = [item for item in json_type if item != "null"]
        return non_null == ["boolean"]
    return False


_NUMERIC_STRING = re.compile(r"^[+-]?(?:\d+(?:\.\d+)?|\.\d+)$")


def _parse_numeric_string(value: str, *, integer: bool) -> int | float | None:
    text = value.strip()
    negative_parens = text.startswith("(") and text.endswith(")")
    if negative_parens:
        text = text[1:-1].strip()
    text = text.replace(",", "").replace(" ", "")
    text = text.replace("$", "").replace("€", "").replace("£", "").replace("¥", "")
    if text.endswith("%"):
        text = text[:-1]
    # Cortex commonly preserves the sentence-ending period printed after an
    # integer amount. A period following a decimal digit remains decimal syntax.
    if text.endswith(".") and text.count(".") == 1:
        text = text[:-1]
    if not _NUMERIC_STRING.fullmatch(text):
        return None
    try:
        number = float(text) if "." in text else int(text)
    except ValueError:
        return None
    if negative_parens:
        number = -abs(number)
    if integer:
        if isinstance(number, float) and not number.is_integer():
            return None
        return int(number)
    return number


_PRINTED_NUMBER = re.compile(
    r"(?<![\w.])\(?(?:[+-]?\$?)?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?%?\)?(?![\w.])"
)


def printed_numeric_tokens(text: str) -> list[str]:
    """Numeric tokens as printed in pdfminer text (commas, %, $, parentheses)."""
    if not text:
        return []
    return [match.group(0) for match in _PRINTED_NUMBER.finditer(text)]


def _canonical_numeric_text(value: Any) -> str | None:
    """Sign + integer + optional fraction, no thousands separators."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, float):
        text = format(value, ".15g")
    else:
        text = str(value).strip()
    parsed = _parse_numeric_string(text, integer=False)
    if parsed is None:
        return None
    negative_parens = text.startswith("(") and text.endswith(")")
    text = text.strip()
    if negative_parens:
        text = text[1:-1].strip()
    text = text.replace(",", "").replace(" ", "")
    text = text.replace("$", "").replace("€", "").replace("£", "").replace("¥", "")
    if text.endswith("%"):
        text = text[:-1]
    if text.endswith(".") and text.count(".") == 1:
        text = text[:-1]
    if not _NUMERIC_STRING.fullmatch(text):
        return None
    if text.startswith("+"):
        text = text[1:]
    if negative_parens and not text.startswith("-"):
        text = "-" + text
    return text


def _compatible_numeric_texts(extracted: str, printed: str) -> bool:
    """True when both share an integer part and one fraction prefixes the other."""

    def parts(text: str) -> tuple[str, str] | None:
        sign = ""
        body = text
        if body.startswith("-"):
            sign = "-"
            body = body[1:]
        if "." in body:
            whole, frac = body.split(".", 1)
        else:
            whole, frac = body, ""
        if not whole.isdigit() and whole != "":
            return None
        if whole == "":
            whole = "0"
        return sign + str(int(whole)), frac

    left = parts(extracted)
    right = parts(printed)
    if left is None or right is None:
        return False
    if left[0] != right[0]:
        return False
    if not left[1] and not right[1]:
        return True
    if not left[1] or not right[1]:
        return False
    return (
        left[1] == right[1]
        or left[1].startswith(right[1])
        or right[1].startswith(left[1])
    )


def reconcile_numeric_leaves(
    value: Any,
    schema: Any,
    page_text: str,
    *,
    path: str = "",
    repairs: list[dict[str, str]] | None = None,
) -> tuple[Any, list[dict[str, str]]]:
    """Replace truncated/padded numeric leaves when page text has one printed match.

    Field identity stays with EXTRACT: we only change digits when every compatible
    printed token collapses to one value. JSON keys are not searched in the page.
    Schema title, if it occurs in the page, narrows the window; otherwise the
    whole page is used.
    """
    if repairs is None:
        repairs = []
    root_schema = schema if isinstance(schema, dict) else {}
    _reconcile_numeric_node(value, schema, root_schema, page_text, path, repairs)
    return value, repairs


def _title_window(page_text: str, schema: Any, root_schema: dict[str, Any]) -> str:
    resolved = _resolve_schema_node(schema, root_schema)
    title = re.sub(r"\s+", " ", str(resolved.get("title") or "")).strip()
    if len(title) < 8:
        return page_text
    match = re.search(re.escape(title), page_text, flags=re.IGNORECASE)
    if not match:
        return page_text
    return page_text[match.end() : match.end() + 500]


def _reconcile_numeric_node(
    value: Any,
    schema: Any,
    root_schema: dict[str, Any],
    page_text: str,
    path: str,
    repairs: list[dict[str, str]],
) -> Any:
    if isinstance(value, dict):
        resolved = _resolve_schema_node(schema, root_schema)
        properties = (
            resolved.get("properties")
            if isinstance(resolved, dict)
            and isinstance(resolved.get("properties"), dict)
            else {}
        )
        for key, child in value.items():
            child_path = f"{path}.{key}" if path else str(key)
            value[key] = _reconcile_numeric_node(
                child, properties.get(key), root_schema, page_text, child_path, repairs
            )
        return value
    if isinstance(value, list):
        resolved = _resolve_schema_node(schema, root_schema)
        items = resolved.get("items") if isinstance(resolved, dict) else None
        for index, item in enumerate(value):
            value[index] = _reconcile_numeric_node(
                item, items, root_schema, page_text, f"{path}[{index}]", repairs
            )
        return value
    numeric_type = _numeric_schema_type(schema, root_schema)
    if numeric_type is None:
        return value
    extracted = _canonical_numeric_text(value)
    if extracted is None:
        return value
    scope = _title_window(page_text, schema, root_schema)
    matches: list[str] = []
    seen: set[str] = set()
    for token in printed_numeric_tokens(scope):
        printed = _canonical_numeric_text(token)
        if printed is None or printed in seen:
            continue
        if numeric_type == "integer" and "." in printed:
            continue
        if _compatible_numeric_texts(extracted, printed):
            seen.add(printed)
            matches.append(printed)
    if len(matches) != 1 or matches[0] == extracted:
        return value
    printed = matches[0]
    repairs.append({"path": path or "$", "from": extracted, "to": printed})
    parsed = _parse_numeric_string(printed, integer=numeric_type == "integer")
    if parsed is None:
        return printed
    if isinstance(value, str):
        return printed
    return parsed


def _schema_defs(schema: Any) -> dict[str, Any]:
    """``$defs`` / ``definitions``, including from a ``{"schema": {...}}`` wrapper.

    The legality walk resolves references, and an unresolvable one is
    indistinguishable from an empty definition -- so these must reach it.
    """
    if not isinstance(schema, dict):
        return {}
    defs = dict(schema.get("$defs") or schema.get("definitions") or {})
    inner = schema.get("schema")
    if isinstance(inner, dict):
        for key in ("$defs", "definitions"):
            nested = inner.get(key)
            if isinstance(nested, dict):
                defs.update(nested)
    return defs


def is_extract_legal(schema: Any) -> bool:
    """True when we should send the schema to AI_EXTRACT as-is (plus scalar stringify).

    References and unions are RESOLVED before judging, so a ``$ref`` to an
    object is judged exactly like an inline ``items: {"type": "object"}``:
    AI_EXTRACT cannot emit object items either way, and treating the two
    differently silently routed whole document classes to an engine that
    returns nothing for them.

    The EXTRACT *payload* stays un-inlined -- see ``for_cortex_extract`` --
    because the inlined form is what Cortex rejects. Resolution here is only
    to decide the engine.
    """
    if not schema:
        return False
    if isinstance(schema, list):
        return True
    if not isinstance(schema, dict):
        return False
    props = schema_properties(schema)
    if not props:
        return all(isinstance(value, (str, dict)) for value in schema.values()) or (
            "type" not in schema and not props
        )
    defs = _schema_defs(schema)
    return all(_extract_legal_defn(defn, defs) for defn in props.values())


def prepare_schema_for_engine(schema: Any, engine: str) -> Any:
    if engine == "ai_complete":
        return for_cortex_complete(schema)
    return for_cortex_extract(schema)


def _extract_legal_defn(defn: Any, defs: dict | None = None) -> bool:
    if isinstance(defn, str):
        return True
    if not isinstance(defn, dict):
        return False
    defs = defs or {}
    indirect = bool(
        defn.get("$ref") or "anyOf" in defn or "oneOf" in defn or "allOf" in defn
    )
    resolved = _simplify(defn, defs)
    if not isinstance(resolved, dict):
        return False
    if indirect and not resolved:
        # _resolve_ref returns {} for a reference it cannot find, which would
        # otherwise read as an untyped (therefore legal) property. An unknown
        # shape goes to the permissive engine rather than being guessed legal.
        return False
    defn = resolved
    json_type = defn.get("type")
    if isinstance(json_type, list):
        non_null = [item for item in json_type if item != "null"]
        json_type = non_null[0] if len(non_null) == 1 else None
        if json_type is None and non_null:
            return all(item in _EXTRACT_SCALARS or item is None for item in non_null)
    if json_type in _EXTRACT_SCALARS or json_type is None:
        return True
    if json_type == "array":
        items = defn.get("items")
        if items is None:
            return True
        if isinstance(items, dict):
            # _simplify already resolved refs/unions inside items above.
            if not items:
                return False  # unknown item shape; do not assume EXTRACT copes
            if items.get("type") == "object" or "properties" in items:
                # An array of objects -- however it was spelled. AI_EXTRACT
                # accepts the call and then emits string items or nothing.
                return False
            item_type = items.get("type")
            return item_type in _EXTRACT_SCALARS or item_type is None
        return False
    if json_type == "object":
        if not is_table_property(defn):
            return False
        return all(
            _extract_legal_defn(col, defs)
            for col in (defn.get("properties") or {}).values()
        )
    return False


def _stringify_number_bool_types(node: Any) -> Any:
    """Rewrite number/bool/integer types to string; keep $ref, anyOf, $defs."""
    if isinstance(node, list):
        return [_stringify_number_bool_types(item) for item in node]
    if not isinstance(node, dict):
        return node
    out: dict[str, Any] = {}
    for key, value in node.items():
        if key == "$ref":
            out[key] = value
        elif isinstance(value, (dict, list)):
            out[key] = _stringify_number_bool_types(value)
        else:
            out[key] = value
    json_type = out.get("type")
    if isinstance(json_type, list):
        out["type"] = [
            "string" if item in {"number", "integer", "boolean"} else item
            for item in json_type
        ]
    elif json_type in {"number", "integer", "boolean"}:
        out["type"] = "string"
    return out


def _scalars_as_string(node: Any) -> Any:
    if isinstance(node, list):
        return [_scalars_as_string(item) for item in node]
    if not isinstance(node, dict):
        return node
    out = dict(node)
    if "properties" in out and isinstance(out["properties"], dict):
        out["properties"] = {
            name: _scalars_as_string(defn) for name, defn in out["properties"].items()
        }
    if "items" in out:
        out["items"] = _scalars_as_string(out["items"])
    json_type = out.get("type")
    if json_type in {"number", "integer", "boolean", "array"}:
        if json_type == "array" and "items" in out:
            hint = out.pop("items")
            extra = f" JSON array of {hint!r}, or empty string if none."
            out["description"] = (str(out.get("description") or "") + extra).strip()
        out["type"] = "string"
    return out


def _simplify(node: Any, defs: dict[str, Any]) -> Any:
    if isinstance(node, list):
        return [_simplify(item, defs) for item in node]
    if not isinstance(node, dict):
        return node

    if "$ref" in node:
        resolved = _resolve_ref(node["$ref"], defs)
        merged = {**resolved, **{k: v for k, v in node.items() if k != "$ref"}}
        return _simplify(merged, defs)

    for key in ("anyOf", "oneOf"):
        if key in node:
            chosen = _unwrap_union(node[key])
            extra = {k: v for k, v in node.items() if k not in {key, "allOf"}}
            merged = {**chosen, **{k: v for k, v in extra.items() if k not in chosen}}
            if "description" in extra:
                merged["description"] = extra["description"]
            if "title" in extra:
                merged["title"] = extra["title"]
            return _simplify(merged, defs)

    if "allOf" in node:
        merged: dict[str, Any] = {}
        for part in node["allOf"]:
            if isinstance(part, dict):
                merged.update(part)
        extra = {k: v for k, v in node.items() if k != "allOf"}
        merged.update(extra)
        return _simplify(merged, defs)

    out: dict[str, Any] = {}
    # `type: ["string", "null"]` carries nullability the same way anyOf does, and
    # _unwrap_type strips it -- record it before that happens.
    declared = node.get("type")
    if (
        isinstance(declared, list)
        and "null" in declared
        and any(item != "null" for item in declared)
    ):
        out[_NULLABLE] = True
    for key, value in node.items():
        if key in _DROP:
            continue
        if key == "properties" and isinstance(value, dict):
            out[key] = {name: _simplify(defn, defs) for name, defn in value.items()}
        elif key == "items":
            out[key] = _simplify(value, defs)
        elif key == "type":
            out[key] = _unwrap_type(value)
        else:
            out[key] = (
                _simplify(value, defs) if isinstance(value, (dict, list)) else value
            )

    json_type = out.get("type")
    if json_type is None:
        if "properties" in out:
            out["type"] = "object"
        elif "items" in out:
            out["type"] = "array"
    return out


def _unwrap_type(value: Any) -> Any:
    if isinstance(value, list):
        non_null = [item for item in value if item != "null"]
        return non_null[0] if len(non_null) == 1 else (non_null or value)
    return value


def _unwrap_union(options: list[Any]) -> dict[str, Any]:
    non_null = [
        item
        for item in options
        if not (isinstance(item, dict) and item.get("type") == "null")
    ]
    chosen = non_null[0] if non_null else (options[0] if options else {})
    result = copy.deepcopy(chosen) if isinstance(chosen, dict) else {"type": chosen}
    # Collapsing the union must not silently make an optional field mandatory.
    # _restore_nullability turns this marker back into an explicit null arm once
    # the rest of the simplification (including stringification) has run.
    if non_null and len(non_null) != len(options):
        result[_NULLABLE] = True
    return result


def _resolve_ref(ref: str, defs: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(ref, str):
        return {}
    if ref.startswith("#/$defs/"):
        name = ref.split("/", 2)[-1]
    elif ref.startswith("#/definitions/"):
        name = ref.split("/", 2)[-1]
    else:
        name = ref.rsplit("/", 1)[-1]
    resolved = defs.get(name)
    return copy.deepcopy(resolved) if isinstance(resolved, dict) else {}
