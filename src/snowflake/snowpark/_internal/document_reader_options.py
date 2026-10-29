#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

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
# parse_mode values a user can choose; "none" (do not parse) is set by the reader itself
# whenever a schema or prompt is given.
_USER_PARSE_MODES = _PARSE_MODES - {"none"}

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
RESPONSE_COLUMN = "RESPONSE"

_PARSE_ERROR_COLUMN = "_DOC_PARSE_ERROR"
_EXTRACT_ERROR_COLUMN = "_DOC_EXTRACT_ERROR"

# The status block the extraction procedure adds to every schema it sends (STATUS_FIELD in
# document_extract_proc.py); a user field of that name would replace it and then be dropped.
_EXTRACTION_STATUS_FIELD = "EXTRACTION_STATUS"

_RESERVED_OUTPUT_COLUMNS = frozenset(
    {
        SOURCE_FILE_COLUMN,
        PAGE_INDEX_COLUMN,
        TOTAL_PAGES_COLUMN,
        CONTENT_COLUMN,
        IMAGES_COLUMN,
        EXTRACTION_SCORES_COLUMN,
        RESPONSE_COLUMN,
        _EXTRACTION_STATUS_FIELD,
    }
)


@dataclass
class ExtractionSpec:
    """A response_format option, normalized once: the output field names it implies
    plus the per-engine call shape. A bare JSON-Schema dict (has a "properties" dict)
    needs its own envelope for each engine -- AI_EXTRACT wants {"schema": {...}}, AI_COMPLETE
    wants {"type": "json", "schema": {...}} (see its docstring's "Structured output with
    response format" example) -- confirmed live against both engines, since neither's
    own docstring documents this shape. AI_EXTRACT's own flat Q&A dict/array forms are
    passed to AI_EXTRACT as given and converted to a string-typed JSON Schema for
    AI_COMPLETE."""

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
        # entry, or a JSON-Schema type union like ["string", "null"], falls back to None
        # (raw VARIANT) rather than raising, same as "no declared type" does.
        if not isinstance(property_schema, dict):
            return None
        json_type = property_schema.get("type")
        if not isinstance(json_type, str):
            return None
        return _JSON_SCHEMA_SCALAR_TYPES.get(json_type)

    @classmethod
    def from_response_format(cls, response_format: Any) -> Optional["ExtractionSpec"]:
        if not response_format:
            return None

        def field_and_question(item: Any) -> Tuple[str, Optional[str]]:
            if isinstance(item, (list, tuple)) and item:
                return str(item[0]), (str(item[1]) if len(item) > 1 else None)
            if isinstance(item, str) and ":" in item:
                name, question = item.split(":", 1)
                return name.strip(), question.strip()
            return str(item), None

        is_json_schema = isinstance(response_format, dict) and isinstance(
            response_format.get("properties"), dict
        )

        if is_json_schema:
            json_schema = response_format
            fields = list(response_format["properties"])
            ai_extract_format = {"schema": response_format}
        else:
            if isinstance(response_format, dict):
                pairs = [(str(k), str(v)) for k, v in response_format.items()]
            elif isinstance(response_format, list):
                pairs = [field_and_question(item) for item in response_format]
            else:
                pairs = []
            # Kept from pairs, not the properties dict, so an exact duplicate name still
            # reaches validate_field_names() instead of collapsing silently.
            fields = [name for name, _ in pairs]
            # AI_COMPLETE only accepts a JSON Schema, so AI_EXTRACT's flat Q&A shapes are
            # converted; AI_EXTRACT itself still gets the shape exactly as given.
            json_schema = {
                "type": "object",
                "properties": {
                    name: (
                        {"type": "string", "description": question}
                        if question
                        else {"type": "string"}
                    )
                    for name, question in pairs
                },
            }
            ai_extract_format = response_format

        properties = json_schema["properties"]
        ai_complete_format = {"type": "json", "schema": json_schema}
        field_types = {field: cls._scalar_type(properties[field]) for field in fields}

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
    extraction_engine: str = "ai_complete"
    extraction: Optional[ExtractionSpec] = None
    prompt: Optional[str] = None

    @property
    def parse_enabled(self) -> bool:
        return self.parse_mode != "none"

    @property
    def extract_enabled(self) -> bool:
        return self.extraction is not None

    @property
    def scores_enabled(self) -> bool:
        """Whether the read returns the EXTRACTION_SCORES column: AI_EXTRACT scores, or
        the confidence scores of a schema read through the extraction procedure (an
        AI_COMPLETE schema read that does not parse first)."""
        return self.extract_enabled and (
            self.extraction_engine == "ai_extract" or not self.parse_enabled
        )

    @property
    def complete_enabled(self) -> bool:
        return self.extraction is not None or self.prompt is not None

    @property
    def read_type(self) -> str:
        """The read shape, for usage tracking (the SQL comment marker in
        document_reader.py and the client telemetry api_call in dataframe_reader.py):
        "schema" with a schema, "prompt" with a prompt and no schema, "parse"
        otherwise."""
        if self.extract_enabled:
            return "schema"
        if self.prompt is not None:
            return "prompt"
        return "parse"

    @property
    def page_rows(self) -> bool:
        return self.parse_enabled and self.row_boundary == "page"

    @property
    def error_stages(self) -> List[Tuple[str, str]]:
        stages = []
        if self.parse_enabled:
            stages.append(("parse", _PARSE_ERROR_COLUMN))
        # A schema or a prompt alone both run AI_COMPLETE, whose error is the "extract"
        # stage either way; extract_enabled would miss a prompt-only read's errors.
        if self.complete_enabled:
            stages.append(("extract", _EXTRACT_ERROR_COLUMN))
        return stages

    @property
    def has_corrupt_record_column(self) -> bool:
        return bool(self.error_stages) and self.mode == "PERMISSIVE"

    @classmethod
    def from_reader_options(
        cls, cur_options: Dict[str, Any]
    ) -> "DocumentReaderOptions":
        """The user options are SCHEMA, PROMPT, PARSE_MODE, ROW_BOUNDARY and
        EXTRACT_IMAGES; any other key is ignored, because everything else is chosen by
        the reader. PARSE_MODE, ROW_BOUNDARY and EXTRACT_IMAGES only apply to a read with
        neither a schema nor a prompt, and EXTRACT_IMAGES needs parse_mode "layout"."""
        options = cls(
            extraction=ExtractionSpec.from_response_format(
                cls._validated_schema(cur_options.get("SCHEMA"))
            ),
            prompt=cls._validated_prompt(cur_options),
        )
        # With a schema or prompt the document goes to AI_COMPLETE as a FILE: parsing
        # flattens layout (table columns, printed section numbers) that the page image keeps.
        # With neither, the parsed text is the read's only output, so parsing stays on.
        if options.complete_enabled:
            options.parse_mode = "none"
        else:
            options.parse_mode = cls.user_choice(
                cur_options, "PARSE_MODE", "layout", _USER_PARSE_MODES
            )
            options.row_boundary = cls.user_choice(
                cur_options, "ROW_BOUNDARY", "document", _ROW_BOUNDARIES
            )
            options.extract_images = cls.user_bool(cur_options, "EXTRACT_IMAGES")
            if options.extract_images and options.parse_mode != "layout":
                raise SnowparkClientExceptionMessages.DF_DOCUMENTS_EXTRACT_IMAGES_NEEDS_LAYOUT(
                    options.parse_mode
                )
        options.validate()
        return options

    @staticmethod
    def _validated_schema(value: Any) -> Any:
        """The SCHEMA option, unchanged. A given schema that is not a dict or a list
        would give no fields and run the model without one, so it is refused."""
        if value and not isinstance(value, (dict, list)):
            raise SnowparkClientExceptionMessages.DF_DOCUMENTS_INVALID_OPTION_TYPE(
                "schema", value, "a dict or a list"
            )
        return value

    @staticmethod
    def _validated_prompt(cur_options: Dict[str, Any]) -> Optional[str]:
        """The PROMPT option, unchanged, or None when it is not set. A prompt that is
        not a string, or is empty or blank, is refused."""
        value = cur_options.get("PROMPT")
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise SnowparkClientExceptionMessages.DF_DOCUMENTS_INVALID_OPTION_TYPE(
                "prompt", value, "a non-empty string"
            )
        return value

    @staticmethod
    def user_choice(
        cur_options: Dict[str, Any], key: str, default: str, valid_values: Iterable[str]
    ) -> str:
        """The lower-cased value the user set for ``key``, or ``default`` when it is not
        set; a value outside ``valid_values`` raises."""
        if key not in cur_options:
            return default
        value = str(cur_options[key]).lower()
        if value not in valid_values:
            raise SnowparkClientExceptionMessages.DF_DOCUMENTS_INVALID_OPTION_VALUE(
                key.lower(), cur_options[key], valid_values
            )
        return value

    @staticmethod
    def user_bool(cur_options: Dict[str, Any], key: str) -> bool:
        """The boolean the user set for ``key`` (a bool, or "true"/"false" in any case),
        or False when it is not set; any other value raises."""
        if key not in cur_options:
            return False
        value = cur_options[key]
        if isinstance(value, bool):
            return value
        text = str(value).lower()
        if text not in ("true", "false"):
            raise SnowparkClientExceptionMessages.DF_DOCUMENTS_INVALID_OPTION_VALUE(
                key.lower(), value, ("true", "false")
            )
        return text == "true"

    def validate(self) -> None:
        # parse_mode/row_boundary/mode/extraction_engine drive our own Python routing
        # and are never sent to Snowflake, so nothing downstream catches an
        # unrecognized value -- everything else is either AISQL's to validate or
        # silently unused when inapplicable (row_boundary/page_filter under
        # parse_mode="none", etc.).
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
