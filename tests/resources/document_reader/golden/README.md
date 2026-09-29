# Synthetic document-reader golden dataset

This test-only dataset contains 10 fictional contracts designed to exercise
document extraction across different document shapes. It contains no customer
documents, names, or terms and is not used by production code.

The corpus varies:

- wording and heading conventions;
- born-digital and mixed image/text pages;
- short documents and a longer exhibit pack;
- prose, bullets, and tables;
- explicit amendments and competing historical dates;
- scalar, clause, and list extraction targets.

## Files

- `source_documents.json`: source text, labels, and page layout instructions.
- `schema.json`: one caller-supplied AI_EXTRACT schema shared by every PDF.
- `expected.json`: expected values, comparison modes, page counts, and checksums.
- `pdfs/`: generated PDF fixtures.
- `generate.py`: deterministic, standard-library-only generator.

## Regenerating

From the repository root:

```shell
python tests/resources/document_reader/golden/generate.py
```

Commit changes to the source JSON and all regenerated artifacts together. The
integrity unit test verifies filenames, labels, checksums, and page counts.

## Label comparison

Fields without a `match_modes` entry are suitable for normalized exact matching.
`normalized_contains` is intended for extracted clause text that may include
surrounding words. `semantic` marks labels where a model may faithfully return a
different ordering or equivalent wording.

The corpus is deliberately not wired into default integration tests because a
full run invokes Cortex once per file. A quality-evaluation job may stage the
files and compare `session.read.documents(..., schema=schema.json)` with
`expected.json`.
