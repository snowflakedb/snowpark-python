#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import hashlib
import json
import re
from pathlib import Path


GOLDEN_ROOT = Path(__file__).parents[1] / "resources" / "document_reader" / "golden"
PDF_DIR = GOLDEN_ROOT / "pdfs"
EXPECTED_NAMES = {
    "01_short_msa.pdf",
    "02_sow_delivery.pdf",
    "03_addendum_new_end_date.pdf",
    "04_alt_headings_msa.pdf",
    "05_taxonomy_tables.pdf",
    "06_signature_page.pdf",
    "07_financial_minimums.pdf",
    "08_permitted_use_list.pdf",
    "09_bilateral_penalties.pdf",
    "10_long_exhibits.pdf",
}
MATCH_MODES = {"normalized_contains", "semantic"}


def _load_json(name):
    return json.loads((GOLDEN_ROOT / name).read_text())


def test_golden_dataset_is_complete_and_self_consistent():
    source = _load_json("source_documents.json")
    schema = _load_json("schema.json")
    labels = _load_json("expected.json")

    source_documents = {
        document["filename"]: document for document in source["documents"]
    }
    schema_fields = set(schema["properties"])

    assert set(source_documents) == EXPECTED_NAMES
    assert set(labels["documents"]) == EXPECTED_NAMES
    assert {path.name for path in PDF_DIR.glob("*.pdf")} == EXPECTED_NAMES
    assert labels["dataset_version"] == source["dataset_version"]

    for name, document in source_documents.items():
        golden = labels["documents"][name]
        assert golden["expected"] == document["expected"]
        assert set(golden["expected"]) <= schema_fields
        assert set(golden["match_modes"]) <= set(golden["expected"])
        assert set(golden["match_modes"].values()) <= MATCH_MODES
        assert golden["page_count"] == len(document["pages"])


def test_generated_pdfs_match_manifest():
    labels = _load_json("expected.json")

    for name, golden in labels["documents"].items():
        payload = (PDF_DIR / name).read_bytes()
        assert payload.startswith(b"%PDF-1.4")
        assert payload.rstrip().endswith(b"%%EOF")
        assert hashlib.sha256(payload).hexdigest() == golden["sha256"]
        assert len(re.findall(rb"/Type /Page\b", payload)) == golden["page_count"]


def test_dataset_includes_mixed_text_and_image_pdf():
    payload = (PDF_DIR / "06_signature_page.pdf").read_bytes()
    assert b"/Subtype /Image" in payload
    assert b"EXECUTED SIGNATURE PAGE" in payload
