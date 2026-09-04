#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import io
import json
from typing import List, Tuple

from snowflake.snowpark.files import SnowflakeFile

PAGE_SEPARATOR: str = "\n\n"


def selected_page_indexes(total_pages: int, page_filter_json: str) -> List[int]:
    """Resolve a JSON-encoded list of {start, end} ranges (0-based, end exclusive)
    into a sorted list of page indexes."""
    if not page_filter_json:
        return list(range(total_pages))

    selected = set()
    for page_range in json.loads(page_filter_json):
        start = max(0, int(page_range["start"]))
        end = min(total_pages, int(page_range["end"]))
        selected.update(range(start, end))
    return sorted(selected)


def extract_selected_pages(
    file_obj, page_filter_json: str
) -> Tuple[int, List[Tuple[int, str]]]:
    """Returns the document's page count and ``(index, text)`` for the selected pages."""
    # pdfminer.six is resolved from Snowflake's Anaconda channel inside the UDTF and
    # is not a client-side dependency, so it is imported at call time.
    from pdfminer.high_level import extract_pages
    from pdfminer.layout import LTTextContainer
    from pdfminer.pdfpage import PDFPage

    # pdfminer makes many small reads and seeks, each of which SnowflakeFile would
    # service against the remote stage, so the document is buffered locally first.
    buffered = io.BytesIO(file_obj.read())

    # Walking the page tree is cheap while layout analysis is not, so the page count
    # is resolved first and only the selected pages are analyzed.
    total_pages = sum(1 for _ in PDFPage.get_pages(buffered))
    indexes = selected_page_indexes(total_pages, page_filter_json)
    # pdfminer reads an empty page_numbers as "every page", so an empty selection
    # has to short-circuit rather than be passed through.
    if not indexes:
        return total_pages, []

    buffered.seek(0)
    texts = (
        "".join(
            element.get_text()
            for element in layout
            if isinstance(element, LTTextContainer)
        )
        for layout in extract_pages(buffered, page_numbers=indexes)
    )
    return total_pages, list(zip(indexes, texts))


class PDFTextReader:
    def process(
        self,
        filename: str,
        row_boundary: str,
        page_filter_json: str,
        mode: str,
    ):
        """
        Extracts text from a staged PDF, yielding
        ``(page_index, total_pages, content, error)`` tuples.

        Args:
            filename: Stage path of the document, e.g. ``@mystage/doc.pdf``.
            row_boundary: ``"document"`` for one row per file, ``"page"`` for one row per page.
            page_filter_json: JSON-encoded list of ``{"start", "end"}`` objects, or ``""``
                to read every page.
            mode: ``"PERMISSIVE"`` yields an error row for an unreadable document,
                ``"FAILFAST"`` aborts the read.
        """
        try:
            with SnowflakeFile.open(
                filename, "rb", require_scoped_url=False
            ) as file_obj:
                total_pages, pages = extract_selected_pages(file_obj, page_filter_json)

            if row_boundary == "page":
                for index, content in pages:
                    yield (index, total_pages, content, None)
            else:
                content = PAGE_SEPARATOR.join(text for _, text in pages)
                yield (None, total_pages, content, None)
        except Exception as e:
            message = f"Failed to extract text from {filename}: {e}"
            if mode == "FAILFAST":
                raise RuntimeError(message)
            yield (None, None, None, message)
