#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from snowflake.snowpark._internal.cortex_document_schema import _unwrap_union
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark.types import (
    BooleanType,
    DataType,
    DoubleType,
    LongType,
    StringType,
)

_PARSE_MODES = frozenset({"layout", "ocr", "text", "none"})
_ROW_BOUNDARIES = frozenset({"document", "page"})
_MODES = frozenset({"PERMISSIVE", "FAILFAST"})
_EXTRACTION_ENGINES = frozenset({"ai_extract", "ai_complete"})

# JSON Schema scalar types mapped to Snowpark equivalents; "array"/"object" are omitted since AI_EXTRACT/AI_COMPLETE already return them as structured VARIANT.
_JSON_SCHEMA_SCALAR_TYPES: Dict[str, DataType] = {
    "string": StringType(),
    "number": DoubleType(),
    "integer": LongType(),
    "boolean": BooleanType(),
}

_DEFAULT_CORRUPT_RECORD_COLUMN = "_document_error"

SOURCE_FILE_COLUMN = "SOURCE_FILE"
PAGE_INDEX_COLUMN = "PAGE_INDEX"
TOTAL_PAGES_COLUMN = "TOTAL_PAGES"
CONTENT_COLUMN = "CONTENT"
IMAGES_COLUMN = "IMAGES"
EXTRACTION_SCORES_COLUMN = "EXTRACTION_SCORES"

_PARSE_ERROR_COLUMN = "_DOC_PARSE_ERROR"
_EXTRACT_ERROR_COLUMN = "_DOC_EXTRACT_ERROR"

_RESERVED_OUTPUT_COLUMNS = frozenset(
    {
        SOURCE_FILE_COLUMN,
        PAGE_INDEX_COLUMN,
        TOTAL_PAGES_COLUMN,
        CONTENT_COLUMN,
        IMAGES_COLUMN,
        EXTRACTION_SCORES_COLUMN,
    }
)


@dataclass
class ExtractionSpec:
    """A response_format option, normalized once: the output field names it implies
    plus the per-engine call shape. A bare JSON-Schema dict (has a "properties" dict)
    needs its own envelope for each engine -- AI_EXTRACT wants {"schema": {...}}, AI_COMPLETE
    wants {"type": "json", "schema": {...}} (see its docstring's "Structured output with
    response format" example) -- confirmed live against both engines, since neither's
    own docstring documents this shape. Every other shape (AI_EXTRACT's own flat Q&A
    dict/array forms) is passed to both engines exactly as given."""

    fields: List[str]
    field_columns: List[str]
    ai_extract_format: Any
    ai_complete_format: Any
    # Per-field output type (keyed by original field name); None means keep the raw VARIANT
    # (composite types, or no declared type in response_format).
    field_types: Dict[str, Optional[DataType]]

    @staticmethod
    def _scalar_type(property_schema: Any) -> Optional[DataType]:
        # property_schema is user-supplied and only loosely validated upstream (it just
        # has to be a dict for is_json_schema to trigger at all) -- a malformed per-field
        # entry falls back to None (raw VARIANT) rather than raising, same as "no
        # declared type" does.
        if not isinstance(property_schema, dict):
            return None
        json_type = property_schema.get("type")
        if json_type is None:
            # for_cortex_extract() deliberately leaves anyOf/oneOf unresolved --
            # its own docstring says inlining them into explicit arrays/objects is
            # what live AI_EXTRACT rejects -- so every nullable field on that path
            # arrives this way instead of as the type: [X, "null"] list the branch
            # below handles. h12's schema is anyOf-wrapped on all 78 of its fields,
            # and with no handling here every one of them resolved to None: nothing
            # got cast in cast_extracted_field(), and the raw VARIANT reached the
            # caller double-JSON-quoted rather than as the string/boolean it
            # declared. That went unnoticed because the eval harness's
            # unwrap_extracted() was silently
            # stripping the extra quoting before scoring, masking the bug rather
            # than exercising the fix. _unwrap_union already resolves this exact
            # shape, so this reuses it instead of re-deriving the same null-member
            # filtering by hand -- hand-rolling it is the mistake this project keeps
            # making (is_extract_legal, the type-list branch below, is_table_property,
            # and looks_generative each did it separately and each needed its own
            # fix). _unwrap_union itself always resolves to its first non-null member
            # no matter how many real members are present, so the ambiguity check --
            # two real members names no single type, and raw VARIANT is the honest
            # answer -- has to happen here, before delegating to it for the actual
            # resolution.
            members = property_schema.get("anyOf") or property_schema.get("oneOf")
            if isinstance(members, list):
                non_null_members = [
                    member
                    for member in members
                    if not (isinstance(member, dict) and member.get("type") == "null")
                ]
                if len(non_null_members) == 1:
                    # The chosen member can itself be an unresolved $ref (no "type"
                    # key at all -- _scalar_type has no access to the root schema's
                    # $defs to chase it, so it stays raw VARIANT, same as any other
                    # undeclared type) or carry its own type: [X, "null"] list, which
                    # the branch immediately below this one already knows how to
                    # collapse.
                    json_type = _unwrap_union(members).get("type")
        if isinstance(json_type, list):
            # simplify_json_schema() spells an optional field as type: [X, "null"],
            # so this union is how every nullable field now arrives. It names one
            # output type, and not resolving it here silently skips the cast in
            # cast_extracted_field() -- which matters most for a field the same
            # preparation stringified, because a VARIANT holding JSON *text*
            # reaches the caller double-encoded rather than as the array it
            # describes. A union with two real members names no single type and
            # still keeps the raw VARIANT.
            candidates = [
                member
                for member in json_type
                if isinstance(member, str) and member != "null"
            ]
            json_type = candidates[0] if len(candidates) == 1 else None
        if not isinstance(json_type, str):
            return None
        return _JSON_SCHEMA_SCALAR_TYPES.get(json_type)

    @classmethod
    def from_response_format(cls, response_format: Any) -> Optional["ExtractionSpec"]:
        if not response_format:
            return None

        def field_name(item: Any) -> str:
            if isinstance(item, (list, tuple)) and item:
                return str(item[0])
            if isinstance(item, str) and ":" in item:
                return item.split(":", 1)[0].strip()
            return str(item)

        is_json_schema = isinstance(response_format, dict) and isinstance(
            response_format.get("properties"), dict
        )

        if is_json_schema:
            fields = list(response_format["properties"])
        elif isinstance(response_format, dict):
            fields = list(response_format)
        elif isinstance(response_format, list):
            fields = [field_name(item) for item in response_format]
        else:
            fields = []

        ai_extract_format = response_format
        ai_complete_format = response_format
        if is_json_schema:
            ai_extract_format = {"schema": response_format}
            ai_complete_format = {"type": "json", "schema": response_format}
            properties = response_format["properties"]
            field_types = {
                field: cls._scalar_type(properties[field]) for field in fields
            }
        else:
            field_types = {field: None for field in fields}

        return cls(
            fields=fields,
            field_columns=[field.upper() for field in fields],
            ai_extract_format=ai_extract_format,
            ai_complete_format=ai_complete_format,
            field_types=field_types,
        )


@dataclass
class DocumentReaderOptions:
    parse_mode: str = "layout"
    row_boundary: str = "document"
    extract_images: bool = False
    page_filter: Optional[list] = None
    mode: str = "PERMISSIVE"
    corrupt_record_column: str = _DEFAULT_CORRUPT_RECORD_COLUMN
    extraction_engine: str = "ai_extract"
    extraction: Optional[ExtractionSpec] = None
    model: Optional[str] = None
    prompt: Optional[str] = None
    extract_scale_factor: float = 1.0
    # True when infer_row_boundary() chose row_boundary rather than the caller.
    # row_boundary="page" is a legitimate thing to ask for -- one row per page is
    # what you want for retrieval chunking -- so the reader must only collapse
    # page rows back into one row per document when it picked page rows itself.
    row_boundary_inferred: bool = False

    @property
    def parse_enabled(self) -> bool:
        return self.parse_mode != "none"

    @property
    def extract_enabled(self) -> bool:
        return self.extraction is not None

    @property
    def page_rows(self) -> bool:
        return self.parse_enabled and self.row_boundary == "page"

    @property
    def error_stages(self) -> List[Tuple[str, str]]:
        stages = []
        if self.parse_enabled:
            stages.append(("parse", _PARSE_ERROR_COLUMN))
        if self.extract_enabled:
            stages.append(("extract", _EXTRACT_ERROR_COLUMN))
        return stages

    @property
    def has_corrupt_record_column(self) -> bool:
        return bool(self.error_stages) and self.mode == "PERMISSIVE"

    @classmethod
    def from_reader_options(
        cls, cur_options: Dict[str, Any]
    ) -> "DocumentReaderOptions":
        defaults = cls()
        options = cls(
            parse_mode=str(cur_options.get("PARSE_MODE", defaults.parse_mode)).lower(),
            row_boundary=str(
                cur_options.get("ROW_BOUNDARY", defaults.row_boundary)
            ).lower(),
            extract_images=bool(
                cur_options.get("EXTRACT_IMAGES", defaults.extract_images)
            ),
            page_filter=cur_options.get("PAGE_FILTER", defaults.page_filter),
            mode=str(cur_options.get("MODE", defaults.mode)).upper(),
            corrupt_record_column=str(
                cur_options.get(
                    "COLUMNNAMEOFCORRUPTRECORD", defaults.corrupt_record_column
                )
            ),
            extraction_engine=str(
                cur_options.get("EXTRACTION_ENGINE", defaults.extraction_engine)
            ).lower(),
            extraction=ExtractionSpec.from_response_format(cur_options.get("SCHEMA")),
            model=cur_options.get("MODEL", defaults.model),
            prompt=cur_options.get("PROMPT", defaults.prompt),
            extract_scale_factor=float(
                cur_options.get("EXTRACT_SCALE_FACTOR", defaults.extract_scale_factor)
            ),
        )
        options.validate()
        return options

    def validate(self) -> None:
        # parse_mode/row_boundary/mode/extraction_engine drive our own Python routing
        # and are never sent to Snowflake, so nothing downstream catches an
        # unrecognized value -- everything else is either AISQL's to validate or
        # silently unused when inapplicable (row_boundary/page_filter under
        # parse_mode="none", model/prompt with no extraction phase, etc.).
        closed_sets: Tuple[Tuple[str, str, Iterable[str]], ...] = (
            ("parse_mode", self.parse_mode, _PARSE_MODES),
            ("row_boundary", self.row_boundary, _ROW_BOUNDARIES),
            ("mode", self.mode, _MODES),
            ("extraction_engine", self.extraction_engine, _EXTRACTION_ENGINES),
        )
        for option_name, value, valid_values in closed_sets:
            if value not in valid_values:
                raise SnowparkClientExceptionMessages.DF_DOCUMENTS_INVALID_OPTION_VALUE(
                    option_name, value, valid_values
                )
        if self.extraction is not None:
            # finalize_errors() anchors the FAILFAST guard on CONTENT when parsing
            # runs, or on the first extracted field column when it doesn't -- an
            # unrecognized response_format shape derives zero fields, and with
            # parse_mode="none" there is then no column at all to anchor on. Every
            # other mode/parse_mode combination tolerates zero fields fine (they
            # just produce a read with no extracted columns), so this is scoped to
            # exactly the combination that has no valid anchor, not banned outright.
            if (
                self.mode == "FAILFAST"
                and not self.parse_enabled
                and not self.extraction.field_columns
            ):
                raise SnowparkClientExceptionMessages.DF_DOCUMENTS_SCHEMA_HAS_NO_FIELDS()
            self.validate_field_names()

    def validate_field_names(self) -> None:
        # with_columns() silently replaces a column of the same name instead of
        # raising, so a field colliding with a reserved output column -- or with
        # another field once both are upper-cased -- would silently overwrite data.
        reserved = _RESERVED_OUTPUT_COLUMNS | {self.corrupt_record_column.upper()}
        seen = set()
        for field in self.extraction.field_columns:
            if field in reserved or field in seen:
                raise SnowparkClientExceptionMessages.DF_DOCUMENTS_SCHEMA_FIELD_NAME_COLLISION(
                    field
                )
            seen.add(field)
