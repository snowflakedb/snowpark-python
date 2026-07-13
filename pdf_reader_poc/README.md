# Snowpark PDF Reader — POC

Adds a real **`session.read.pdf(...)`** method to the Snowpark Python client:
point it at a stage of PDFs, give an output schema, get back a typed DataFrame
ready to save as a table. The extraction prompt is generated from the schema —
you don't write one.

```python
df = session.read.pdf("@invoices/2026/", schema=invoice_schema, mode="auto")
df.write.save_as_table("INVOICE_FACTS")
```

## Where the code lives

The implementation is built **into the snowpark library**, not this directory:

| location | what |
|---|---|
| `~/snowpark-python/src/snowflake/snowpark/dataframe_reader.py` | `DataFrameReader.pdf()` — the public method (reads options, validates, delegates). |
| `~/snowpark-python/src/snowflake/snowpark/_internal/pdf_reader_utils.py` | the orchestration (schema translation, four parse backends, AI_COMPLETE, eager materialize). |
| `snowpark-pdf-reader-poc/demo.py` | this dir — a runnable harness that exercises the editable-installed library. |

## Pipeline

```
stage of PDFs
  ──▶ parse step   (mode: text | parse | direct | auto)
  ──▶ AI_COMPLETE  (schema ─▶ typed columns)
  ──▶ write to target table (session temp table if unspecified)
  ──▶ return a DataFrame over that table
```

Returned columns: `file_url`, the schema fields (typed), `_raw_response`,
`_parse_failed`.

## Modes

Named for the parse strategy / cost point, not the backend:

| mode | parse backend | for |
|---|---|---|
| `text` | pdfplumber UDF → AI_COMPLETE | digital PDFs; cheap, no parse tokens. **Scanned PDFs yield ~empty text.** |
| `parse` | AI_PARSE_DOCUMENT → AI_COMPLETE | robust: layout, tables, OCR, scanned. Billed per page. |
| `direct` | none → multimodal AI_COMPLETE | simple reason-from-image; **no inspectable intermediate**, least auditable. |
| `auto` | probe → OCR only the scans → AI_COMPLETE | mixed/unknown corpora. AI_PARSE cost ∝ scanned subset, not the whole corpus. |

`auto` is a two-pass structure on purpose: pass 1 (cheap pdfplumber probe) is
**materialized**, then pass 2 runs `AI_PARSE_DOCUMENT` only on the rows flagged
`needs_ocr` — deliberately *not* a `CASE WHEN needs_ocr THEN AI_PARSE(...)`,
since short-circuiting isn't guaranteed for an expensive volatile function.

The output `schema` is the contract: it drives the generated extraction prompt,
the structured output, and the typed cast — you don't hand-write a prompt.
`mode` / `model` / `target` / `threshold` and an optional supplementary `prompt`
can be set via `.option()` or as `pdf()` kwargs (kwargs win). `schema` is a kwarg
only — it does **not** go through `.option()`, and it's distinct from
`.schema()` (which is an input-*file* schema for CSV/JSON/etc.).

## Run it

```bash
python3 -m venv .venv && . .venv/bin/activate

# editable-install the library with the new read.pdf(), then the demo deps
pip install -e ~/snowpark-python
pip install -r requirements.txt

# via ~/.snowflake/connections.toml
python demo.py --connection my_dev_conn --mode text

# or via env vars
export SNOWFLAKE_ACCOUNT=... SNOWFLAKE_USER=... SNOWFLAKE_DATABASE=... SNOWFLAKE_SCHEMA=...
export SNOWFLAKE_WAREHOUSE=... SNOWFLAKE_ROLE=...
python demo.py --mode parse
```

`demo.py` generates a couple of digital invoice PDFs, uploads them to a stage
with a directory table, and runs `session.read.pdf`. Try
`--mode text|parse|direct|auto`. Start with `--mode parse` — pure Cortex, no
UDF/package dependency.

**Requirements on the account**
- Cortex enabled (`AI_COMPLETE`, `AI_PARSE_DOCUMENT`).
- For `text`/`auto`: `pdfplumber` resolvable in the UDF sandbox — Snowflake
  Anaconda channel or the **artifact repository**. (The managed `parse` mode has
  no third-party dependency.)

## Verify these two Cortex call sites against your account

Cortex signatures move; both are isolated in `pdf_reader_utils.py`
(`_sql_complete`, `_sql_parse`) so you tweak them in one place:

1. **`AI_COMPLETE` structured output** — `response_format` argument carrying a
   JSON schema (`{"type":"json","schema":{…}}`). Confirm the arg name/shape.
2. **`AI_PARSE_DOCUMENT`** — `AI_PARSE_DOCUMENT(TO_FILE('@stage', path),
   {'mode':'OCR'})`, read via `:content`. Confirm the options object + result.

## Aggressive demo — real S&P 500 earnings

`sp500_earnings_demo.py` sources the last 4 quarters of earnings **press
releases** live from SEC EDGAR (8-K, item 2.02), renders each to a text-layer
PDF, uploads them, and extracts key financials (revenue, net income, diluted
EPS, period end) via `session.read.pdf` into a table.

```bash
# smoke test — ~10 well-known tickers (SAMPLE_TICKERS)
python sp500_earnings_demo.py --connection prod3

# full S&P 500 (~500 companies, fetched from Wikipedia)
python sp500_earnings_demo.py --connection prod3 --sp500

# cap for testing / pick a mode / choose the output table
python sp500_earnings_demo.py --connection prod3 --sp500 --limit 25 \
    --mode text --target TESTDB.PUBLIC.SP500_EARNINGS_FACTS
```

Sourced PDFs cache to `--out-dir` (resumable), and failures are per-company
isolated. Verified end-to-end: the full-index run pulled **1,998 press releases
across ~500 companies, 98% with extracted financials**. (`total_revenue` etc.
are captured as-reported strings; `period_end_date`/`diluted_eps` are typed.
Note: some item-2.02 8-Ks are segment/other filings, not the headline release,
so a small fraction of rows carry non-headline figures.)

## Known limitations (POC scope — see the PRD)

- **Eager.** `read.pdf` executes and materializes on the call, unlike other
  `read.*` methods (lazy). Deliberate: the plan holds volatile, expensive AI
  calls that a lazy DataFrame would re-run and re-bill on every action.
- **No AST emission.** `pdf()` doesn't emit reader AST — fine for local/live
  execution; revisit for phase0/stored-proc.
- **No large-document chunking.** Each document must fit one `AI_COMPLETE` call;
  long docs truncate silently. Product-level work.
- **No directory sampling.** `auto` detects per file; the "sample ~10%, and if
  mostly scanned skip the probe and OCR everything" optimization is deferred.
- **Per-file scan routing**, not per-page (hybrid docs route as a whole file).
- **Not tested end-to-end here** — authored against Snowpark + Cortex APIs; run
  it against a Cortex-enabled dev account.
