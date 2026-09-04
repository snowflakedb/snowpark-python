#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages

_PARSE_MODES = frozenset({"layout", "ocr", "text", "none"})
_ROW_BOUNDARIES = frozenset({"document", "page"})
_MODES = frozenset({"PERMISSIVE", "FAILFAST"})
_EXTRACTION_ENGINES = frozenset({"ai_extract", "ai_complete"})

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
    plus the per-engine call shape (AI_COMPLETE needs its own {"type": "json", "schema":
    {...}} envelope -- see its docstring's "Structured output with response format"
    example -- while AI_EXTRACT takes the value as given, in any shape it documents)."""

    fields: List[str]
    field_columns: List[str]
    ai_extract_format: Any
    ai_complete_format: Any

    @classmethod
    def from_response_format(cls, response_format: Any) -> Optional["ExtractionSpec"]:
        if not response_format:
            return None

        def field_name(item: Any) -> str:
            if isinstance(item, (list, tuple)) and item:
                return item[0]
            if isinstance(item, str) and ":" in item:
                return item.split(":", 1)[0].strip()
            return item

        if isinstance(response_format, dict):
            properties = response_format.get("properties")
            fields = (
                list(properties)
                if isinstance(properties, dict)
                else list(response_format)
            )
        elif isinstance(response_format, list):
            fields = [field_name(item) for item in response_format]
        else:
            fields = []

        ai_complete_format = response_format
        if isinstance(response_format, dict) and isinstance(
            response_format.get("properties"), dict
        ):
            ai_complete_format = {"type": "json", "schema": response_format}

        return cls(
            fields=fields,
            field_columns=[field.upper() for field in fields],
            ai_extract_format=response_format,
            ai_complete_format=ai_complete_format,
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
