#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
"""Generate the synthetic document-reader golden dataset.

The generated PDFs intentionally use only Python's standard library. Run:

    python tests/resources/document_reader/golden/generate.py

The source facts and page text live in source_documents.json. This script writes
schema.json, expected.json, and pdfs/*.pdf deterministically.
"""

from __future__ import annotations

import hashlib
import json
import textwrap
import zlib
from pathlib import Path
from typing import Any


ROOT = Path(__file__).parent
SOURCE_PATH = ROOT / "source_documents.json"
PDF_DIR = ROOT / "pdfs"

PAGE_WIDTH = 612
PAGE_HEIGHT = 792

SCHEMA = {
    "type": "object",
    "properties": {
        "vendor_name": {
            "type": "string",
            "description": "What is the full legal name of the vendor or supplier?",
        },
        "start_date": {
            "type": "string",
            "description": "What is the contract effective or start date?",
        },
        "end_date": {
            "type": "string",
            "description": "What is the contract expiration or end date?",
        },
        "auto_renew": {
            "type": "string",
            "description": "Does the contract automatically renew, and for what term?",
        },
        "termination_clause": {
            "type": "string",
            "description": "What does the contract say about early termination?",
        },
        "delivery_window": {
            "type": "string",
            "description": "What delivery schedule, cadence, or time window is required?",
        },
        "annual_minimum": {
            "type": "string",
            "description": "What annual spending, purchase, or volume minimum is required?",
        },
        "permitted_uses": {
            "type": "array",
            "items": {"type": "string"},
            "description": "List: What uses of the supplied data are permitted?",
        },
        "penalty_supplier": {
            "type": "string",
            "description": "What penalty applies when the supplier fails its obligations?",
        },
        "penalty_customer": {
            "type": "string",
            "description": "What penalty applies when the customer fails its obligations?",
        },
    },
}


def _pdf_string(value: str) -> str:
    return (
        value.replace("\\", "\\\\")
        .replace("(", "\\(")
        .replace(")", "\\)")
        .encode("latin-1", "replace")
        .decode("latin-1")
    )


def _wrapped_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        if not raw_line:
            lines.append("")
            continue
        indent = len(raw_line) - len(raw_line.lstrip())
        lines.extend(
            textwrap.wrap(
                raw_line,
                width=82,
                initial_indent=" " * indent,
                subsequent_indent=" " * indent,
                replace_whitespace=False,
                drop_whitespace=False,
            )
            or [""]
        )
    return lines[:47]


def _text_stream(text: str) -> bytes:
    commands = ["BT", "/F1 10 Tf", "64 746 Td", "14 TL"]
    for line in _wrapped_lines(text):
        commands.append(f"({_pdf_string(line)}) Tj")
        commands.append("T*")
    commands.append("ET")
    return "\n".join(commands).encode("latin-1")


def _signature_bitmap(width: int = 320, height: int = 120) -> bytes:
    """Return a deterministic grayscale image resembling a signed approval box."""
    pixels = bytearray([255] * (width * height))

    def darken(x: int, y: int, radius: int = 1) -> None:
        for dy in range(-radius, radius + 1):
            for dx in range(-radius, radius + 1):
                px, py = x + dx, y + dy
                if 0 <= px < width and 0 <= py < height:
                    pixels[py * width + px] = 20

    for x in range(18, width - 18):
        darken(x, 94, 0)
    for x in range(35, 265):
        y = 57 + int(17 * __import__("math").sin(x / 13))
        darken(x, y, 1)
        if x % 3 == 0:
            darken(x, y - 7, 0)
    for y in range(15, height - 15):
        darken(15, y, 0)
        darken(width - 16, y, 0)
    for x in range(15, width - 15):
        darken(x, 15, 0)
        darken(x, height - 16, 0)
    return bytes(pixels)


def _build_pdf(pages: list[dict[str, Any]]) -> bytes:
    # Object 1: catalog, 2: page tree, 3: Helvetica font.
    objects: list[bytes | None] = [
        None,
        b"<< /Type /Catalog /Pages 2 0 R >>",
        None,
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    page_ids: list[int] = []

    for page in pages:
        page_id = len(objects)
        page_ids.append(page_id)
        objects.append(None)

        resources = "<< /Font << /F1 3 0 R >>"
        if page.get("image_only"):
            bitmap = zlib.compress(_signature_bitmap())
            image_id = len(objects)
            objects.append(
                (
                    "<< /Type /XObject /Subtype /Image /Width 320 /Height 120 "
                    "/ColorSpace /DeviceGray /BitsPerComponent 8 /Filter /FlateDecode "
                    f"/Length {len(bitmap)} >>\nstream\n"
                ).encode("ascii")
                + bitmap
                + b"\nendstream"
            )
            resources += f" /XObject << /Im1 {image_id} 0 R >>"
            stream = (
                b"BT /F1 12 Tf 72 690 Td (EXECUTED SIGNATURE PAGE) Tj ET\n"
                b"q 320 0 0 120 146 400 cm /Im1 Do Q"
            )
        else:
            stream = _text_stream(page["text"])
        resources += " >>"

        content_id = len(objects)
        objects.append(
            f"<< /Length {len(stream)} >>\nstream\n".encode("ascii")
            + stream
            + b"\nendstream"
        )
        objects[page_id] = (
            f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {PAGE_WIDTH} {PAGE_HEIGHT}] "
            f"/Resources {resources} /Contents {content_id} 0 R >>"
        ).encode("ascii")

    kids = " ".join(f"{page_id} 0 R" for page_id in page_ids)
    objects[2] = (f"<< /Type /Pages /Kids [{kids}] /Count {len(page_ids)} >>").encode(
        "ascii"
    )

    output = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = [0]
    for object_id, body in enumerate(objects[1:], start=1):
        offsets.append(len(output))
        output.extend(f"{object_id} 0 obj\n".encode("ascii"))
        output.extend(body or b"")
        output.extend(b"\nendobj\n")

    xref = len(output)
    output.extend(f"xref\n0 {len(objects)}\n".encode("ascii"))
    output.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        output.extend(f"{offset:010d} 00000 n \n".encode("ascii"))
    output.extend(
        (
            f"trailer\n<< /Size {len(objects)} /Root 1 0 R >>\n"
            f"startxref\n{xref}\n%%EOF\n"
        ).encode("ascii")
    )
    return bytes(output)


def main() -> None:
    source = json.loads(SOURCE_PATH.read_text())
    documents = source["documents"]
    PDF_DIR.mkdir(parents=True, exist_ok=True)

    expected: dict[str, Any] = {
        "dataset_version": source["dataset_version"],
        "description": source["description"],
        "documents": {},
    }
    expected_names = {document["filename"] for document in documents}
    for existing in PDF_DIR.glob("*.pdf"):
        if existing.name not in expected_names:
            existing.unlink()

    for document in documents:
        pdf = _build_pdf(document["pages"])
        filename = document["filename"]
        (PDF_DIR / filename).write_bytes(pdf)
        expected["documents"][filename] = {
            "expected": document["expected"],
            "match_modes": document.get("match_modes", {}),
            "page_count": len(document["pages"]),
            "sha256": hashlib.sha256(pdf).hexdigest(),
        }

    (ROOT / "schema.json").write_text(json.dumps(SCHEMA, indent=2) + "\n")
    (ROOT / "expected.json").write_text(json.dumps(expected, indent=2) + "\n")


if __name__ == "__main__":
    main()
