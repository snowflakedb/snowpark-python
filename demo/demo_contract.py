"""
demo_contract.py — exercises session.read.documents() on contract PDFs

Two examples:
  1. Extract tables and figures from vendor contracts
     (layout mode, per-page, filters to pages with structure)

  2. Extract contract fields from all contract PDFs on the stage
     (layout mode, document-level, diversified categories:
      new contract / renewal / schema update / financial terms / permissive usage)
"""

from snowflake.snowpark import Session

STAGE = "@admindb_mayliu.public.pdf_mock/contracts"
MODEL = "claude-4-sonnet"


def make_session() -> Session:
    return Session.builder.config("connection_name", "myconnection").create()


# ===========================================================================
# Example 1 — tables and figures from contract PDFs
# ===========================================================================

STRUCTURE_SCHEMA = {
    "type": "json",
    "schema": {
        "type": "object",
        "properties": {
            "tables_on_page": {
                "type": "string",
                "description": (
                    "Describe every table on this page: its title/caption, "
                    "column headers, and a 1-sentence summary of what it contains. "
                    "Null if no tables present."
                ),
            },
            "figures_on_page": {
                "type": "string",
                "description": (
                    "Describe every figure, diagram, or exhibit on this page: "
                    "its label, caption, and what it depicts.  Null if none."
                ),
            },
            "has_table": {"type": "string", "description": "true or false"},
            "has_figure": {"type": "string", "description": "true or false"},
        },
    },
}


def example1_tables_and_figures(session: Session) -> None:
    print("\n── Example 1: tables + figures from contract PDFs ───────────────")
    df = (
        session.read.option(
            "document_mode", "layout"
        )  # layout preserves table markdown
        .option("model", MODEL)
        .option(
            "prompt",
            "These are vendor data services contracts. "
            "Identify any pricing tables, SLA tables, "
            "schema/field tables, and any figures or exhibits.",
        )
        .documents(STAGE, schema=STRUCTURE_SCHEMA)
    )
    # Show only pages that contain tables or figures
    df.filter((df["HAS_TABLE"] == "true") | (df["HAS_FIGURE"] == "true")).select(
        "SOURCE_FILE",
        "PAGE_INDEX",
        "TOTAL_PAGES",
        "TABLES_ON_PAGE",
        "FIGURES_ON_PAGE",
        "CONFIDENCE",
    ).show(50, max_width=200)


# ===========================================================================
# Example 2 — extract contract fields across diversified contract types
# ===========================================================================

CONTRACT_SCHEMA = {
    "type": "json",
    "schema": {
        "type": "object",
        "properties": {
            "contract_category": {
                "type": "string",
                "description": (
                    "Classify as one of: new contract, renewal, schema update, "
                    "financial terms, permissive usage"
                ),
            },
            "vendor_name": {"type": "string"},
            "start_date": {"type": "string", "description": "YYYY-MM-DD"},
            "end_date": {"type": "string", "description": "YYYY-MM-DD"},
            "auto_renewal_notice_days": {"type": "string", "description": "integer"},
            "termination_clause": {"type": "string", "description": "1 sentence"},
            "early_termination_fee": {"type": "string"},
            "annual_minimum_usd": {"type": "string", "description": "number only"},
            "permissible_use": {"type": "string"},
            "delivery_slas": {"type": "string"},
            "penalty_summary": {"type": "string"},
        },
    },
}


def example2_contract_extraction(session: Session) -> None:
    print("\n── Example 2: contract field extraction (document-level) ────────")
    # split_by_page=False: aggregate all pages per document before extraction.
    # Contract fields span multiple pages — document-level is more accurate.
    df = (
        session.read.option("document_mode", "layout")
        .option("split_by_page", "false")
        .option("model", MODEL)
        .option(
            "prompt",
            "These are vendor data services contracts between "
            "Merkle Inc. and various data providers. "
            "Classify the contract category and extract all fields.",
        )
        .documents(STAGE, schema=CONTRACT_SCHEMA)
    )
    df.select(
        "SOURCE_FILE",
        "CONTRACT_CATEGORY",
        "VENDOR_NAME",
        "START_DATE",
        "END_DATE",
        "ANNUAL_MINIMUM_USD",
        "TERMINATION_CLAUSE",
        "CONFIDENCE",
    ).show(20, max_width=200)


# ===========================================================================

if __name__ == "__main__":
    session = make_session()
    try:
        example1_tables_and_figures(session)
        example2_contract_extraction(session)
    finally:
        session.close()
