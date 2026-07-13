#!/usr/bin/env python3
"""Runnable demo for the Snowpark PDF Reader POC.

It (1) generates a couple of sample invoice PDFs locally, (2) creates a stage
with a directory table and uploads them, (3) runs read_pdf() in the mode you
pick, and (4) prints the resulting typed DataFrame.

Connect either way:
    # via ~/.snowflake/connections.toml
    python demo.py --connection my_dev_conn --mode text

    # via env vars (SNOWFLAKE_ACCOUNT/USER/PASSWORD or _AUTHENTICATOR, ROLE,
    # WAREHOUSE, DATABASE, SCHEMA)
    python demo.py --mode parse

The account must have Cortex (AI_COMPLETE / AI_PARSE_DOCUMENT) enabled, and for
`text`/`auto` modes, pdfplumber must be resolvable in the UDF sandbox (Snowflake
Anaconda channel, or your artifact repository).
"""

import argparse
import os
import tempfile

from snowflake.snowpark import Session
from snowflake.snowpark.types import (
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
)

# read.pdf() lives in the (editable-installed) snowpark library now, not here.

STAGE = "PDF_READER_POC_STAGE"

INVOICE_SCHEMA = StructType([
    StructField("invoice_number", StringType()),
    StructField("vendor", StringType()),
    StructField("total_usd", DecimalType(18, 2)),
    StructField("issued_on", DateType()),
])

SAMPLE_INVOICES = [
    {"num": "INV-2026-0042", "vendor": "Acme Widgets LLC",
     "total": "1240.50", "date": "2026-03-14"},
    {"num": "INV-2026-0043", "vendor": "Globex Corporation",
     "total": "87999.00", "date": "2026-03-19"},
]


def make_sample_pdfs(out_dir: str) -> list[str]:
    """Write a couple of digital (text-layer) invoice PDFs with reportlab."""
    from reportlab.lib.pagesizes import letter
    from reportlab.pdfgen import canvas

    paths = []
    for inv in SAMPLE_INVOICES:
        p = os.path.join(out_dir, f"{inv['num']}.pdf")
        c = canvas.Canvas(p, pagesize=letter)
        y = 720
        for line in (
            "INVOICE",
            f"Invoice Number: {inv['num']}",
            f"Vendor: {inv['vendor']}",
            f"Issued On: {inv['date']}",
            f"Total Due (USD): ${inv['total']}",
            "",
            "Thank you for your business.",
        ):
            c.drawString(72, y, line)
            y -= 24
        c.showPage()
        c.save()
        paths.append(p)
    return paths


def build_session(args) -> Session:
    # Force JSON result format so result fetching works even where the Arrow
    # native lib is unavailable in the venv.
    sp = {"session_parameters": {"PYTHON_CONNECTOR_QUERY_RESULT_FORMAT": "JSON"}}
    if args.connection:
        return (Session.builder
                .config("connection_name", args.connection)
                .configs(sp).create())
    cfg = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ.get("SNOWFLAKE_PASSWORD"),
        "authenticator": os.environ.get("SNOWFLAKE_AUTHENTICATOR", "snowflake"),
        "role": os.environ.get("SNOWFLAKE_ROLE"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
        "database": os.environ["SNOWFLAKE_DATABASE"],
        "schema": os.environ["SNOWFLAKE_SCHEMA"],
        **sp,
    }
    return Session.builder.configs({k: v for k, v in cfg.items() if v}).create()


def stage_samples(session: Session, pdf_paths: list[str]) -> None:
    # SNOWFLAKE_SSE (server-side encryption) is required: AI_PARSE_DOCUMENT
    # rejects client-side-encrypted stages. TEMPORARY keeps zero persistent
    # footprint (matters when pointed at a prod account).
    session.sql(f"CREATE OR REPLACE TEMPORARY STAGE {STAGE} "
                f"DIRECTORY = (ENABLE = TRUE) "
                f"ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')").collect()
    for p in pdf_paths:
        session.file.put(p, f"@{STAGE}", auto_compress=False, overwrite=True)
    session.sql(f"ALTER STAGE {STAGE} REFRESH").collect()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--connection", help="connections.toml profile name")
    ap.add_argument("--mode", choices=["text", "parse", "direct", "auto"],
                    default="text")
    ap.add_argument("--model", default="llama3.1-70b")
    args = ap.parse_args()

    session = build_session(args)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            pdfs = make_sample_pdfs(tmp)
            stage_samples(session, pdfs)

        # schema is the contract; the extraction prompt is generated from it.
        df = session.read.pdf(
            f"@{STAGE}/",
            INVOICE_SCHEMA,
            mode=args.mode,
            model=args.model,
        )
        print(f"\n=== read_pdf(mode={args.mode!r}) result ===")
        df.show(n=20, max_width=200)
    finally:
        session.close()


if __name__ == "__main__":
    main()
