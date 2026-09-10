#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import json
import os
from unittest.mock import patch

import pytest

from snowflake.snowpark._internal.pdf_reader import (
    PAGE_SEPARATOR,
    PDFTextReader,
    extract_selected_pages,
    selected_page_indexes,
)
from tests.utils import TestFiles

RESOURCES_PATH = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "../resources")
)
TEST_FILES = TestFiles(RESOURCES_PATH)

DOC_PAGE_COUNT = 3


def _page_filter(*ranges):
    return json.dumps([{"start": start, "end": end} for start, end in ranges])


def test_selected_page_indexes_empty_filter_selects_every_page():
    assert selected_page_indexes(5, "") == [0, 1, 2, 3, 4]


def test_selected_page_indexes_single_range():
    assert selected_page_indexes(5, _page_filter((1, 3))) == [1, 2]


def test_selected_page_indexes_multiple_ranges_are_merged_and_sorted():
    assert selected_page_indexes(5, _page_filter((3, 5), (0, 2))) == [0, 1, 3, 4]


def test_selected_page_indexes_overlapping_ranges_deduplicate():
    assert selected_page_indexes(5, _page_filter((0, 3), (1, 4))) == [0, 1, 2, 3]


def test_selected_page_indexes_clamps_out_of_bounds_range():
    assert selected_page_indexes(3, _page_filter((-2, 10))) == [0, 1, 2]


def test_selected_page_indexes_empty_range_selects_nothing():
    assert selected_page_indexes(5, _page_filter((2, 2))) == []


def test_extract_selected_pages_returns_every_page_by_default():
    with open(TEST_FILES.test_doc_pdf, "rb") as f:
        total_pages, pages = extract_selected_pages(f, "")

    assert total_pages == DOC_PAGE_COUNT
    assert [index for index, _ in pages] == [0, 1, 2]
    assert all(text.strip() for _, text in pages)


def test_extract_selected_pages_honors_page_filter():
    with open(TEST_FILES.test_doc_pdf, "rb") as f:
        total_pages, pages = extract_selected_pages(f, _page_filter((0, 2)))

    assert total_pages == DOC_PAGE_COUNT
    assert [index for index, _ in pages] == [0, 1]


def test_extract_selected_pages_empty_selection_returns_no_pages():
    with open(TEST_FILES.test_doc_pdf, "rb") as f:
        total_pages, pages = extract_selected_pages(f, _page_filter((2, 2)))

    assert total_pages == DOC_PAGE_COUNT
    assert pages == []


def _mock_snowflake_file_open(local_path):
    """SnowflakeFile.open() is only meaningful inside a real UDTF; the rest of
    PDFTextReader.process() is plain Python and doesn't need a live session, as
    long as SnowflakeFile.open() is replaced with a local file handle."""
    return patch(
        "snowflake.snowpark._internal.pdf_reader.SnowflakeFile.open",
        return_value=open(local_path, "rb"),
    )


def test_process_document_boundary_joins_pages():
    with _mock_snowflake_file_open(TEST_FILES.test_doc_pdf):
        rows = list(
            PDFTextReader().process("@stage/doc.pdf", "document", "", "PERMISSIVE")
        )

    assert len(rows) == 1
    index, total_pages, content, error = rows[0]
    assert index is None
    assert total_pages == DOC_PAGE_COUNT
    assert error is None
    assert content.count(PAGE_SEPARATOR) == DOC_PAGE_COUNT - 1


def test_process_page_boundary_yields_one_row_per_page():
    with _mock_snowflake_file_open(TEST_FILES.test_doc_pdf):
        rows = list(PDFTextReader().process("@stage/doc.pdf", "page", "", "PERMISSIVE"))

    assert [r[0] for r in rows] == [0, 1, 2]
    assert all(r[1] == DOC_PAGE_COUNT for r in rows)
    assert all(r[3] is None for r in rows)


def test_process_page_boundary_honors_page_filter():
    with _mock_snowflake_file_open(TEST_FILES.test_doc_pdf):
        rows = list(
            PDFTextReader().process(
                "@stage/doc.pdf", "page", _page_filter((0, 2)), "PERMISSIVE"
            )
        )

    assert [r[0] for r in rows] == [0, 1]


def test_process_permissive_yields_an_error_row_for_an_unreadable_file():
    with _mock_snowflake_file_open(TEST_FILES.test_audio_ogg):
        rows = list(
            PDFTextReader().process("@stage/audio.ogg", "document", "", "PERMISSIVE")
        )

    assert len(rows) == 1
    index, total_pages, content, error = rows[0]
    assert index is None
    assert total_pages is None
    assert content is None
    assert "audio.ogg" in error


def test_process_failfast_raises_for_an_unreadable_file():
    with _mock_snowflake_file_open(TEST_FILES.test_audio_ogg):
        with pytest.raises(RuntimeError, match="Failed to extract text"):
            list(
                PDFTextReader().process("@stage/audio.ogg", "document", "", "FAILFAST")
            )
