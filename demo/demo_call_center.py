"""
demo_call_center.py — exercises session.read.documents() on call center transcripts

Two examples:
  1. Extract structured call metadata from all transcripts
     (text mode — client-side direct read, no Snowflake AI parse cost)
     Includes call type classification: billing / service / emergency / technical / other

  2. Richer extraction with summaries + Q&A over results
     (ocr mode — AI_PARSE_DOCUMENT server-side)
"""

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

STAGE = "@admindb_mayliu.public.pdf_mock/call_center"
MODEL = "claude-4-sonnet"


def make_session() -> Session:
    return Session.builder.config("connection_name", "myconnection").create()


# ===========================================================================
# Example 1 — structured metadata + call type classification (text mode)
# ===========================================================================

CALL_SCHEMA = {
    "type": "json",
    "schema": {
        "type": "object",
        "properties": {
            "customer_id": {
                "type": "string",
                "description": "Customer ID from header, e.g. CUST-48291",
            },
            "representative_id": {
                "type": "string",
                "description": "Representative ID from header, e.g. REP-042",
            },
            "category": {
                "type": "string",
                "description": "Call category, e.g. Billing Dispute, "
                "New Service Activation, Emergency - Gas Leak",
            },
            "call_type": {
                "type": "string",
                "description": (
                    "High-level call type — exactly one of: "
                    "billing (billing dispute, inquiry, payment arrangement), "
                    "service (new service, cancellation, upgrade, address change), "
                    "emergency (gas leak, medical, outage with life support), "
                    "technical (portal, app, website issues), "
                    "other"
                ),
            },
            "sentiment": {
                "type": "string",
                "description": "Overall customer sentiment: Positive, Neutral, "
                "Frustrated, Angry, Scared, Relieved, or Stressed",
            },
            "summary": {
                "type": "string",
                "description": "2-3 sentence summary of what happened "
                "and how the call was resolved",
            },
        },
    },
}


def example1_call_metadata(session: Session) -> None:
    print("\n── Example 1: call metadata + type classification (text mode) ───")
    # text mode: client-side direct read of .txt files — zero Snowflake AI parse cost.
    # split_by_page=False: each .txt is one complete call record.
    df = (
        session.read.option("document_mode", "text")
        .option("split_by_page", "false")
        .option("model", MODEL)
        .option(
            "prompt",
            "This is a call center transcript between a utility "
            "company representative and a customer.",
        )
        .documents(STAGE, schema=CALL_SCHEMA)
    )
    df.select(
        "SOURCE_FILE",
        "CUSTOMER_ID",
        "REPRESENTATIVE_ID",
        "CATEGORY",
        "CALL_TYPE",
        "SENTIMENT",
        "CONFIDENCE",
    ).show(20, max_width=200)


# ===========================================================================
# Example 2 — full summaries + Q&A (ocr mode)
# ===========================================================================


def example2_summaries_and_qa(session: Session) -> None:
    print("\n── Example 2: summaries + Q&A (ocr mode) ────────────────────────")
    # ocr mode: AI_PARSE_DOCUMENT server-side — consistent with the contract demos.
    df = (
        session.read.option("document_mode", "ocr")
        .option("split_by_page", "false")
        .option("model", MODEL)
        .option(
            "prompt",
            "This is a call center transcript between a utility "
            "company representative and a customer.",
        )
        .documents(STAGE, schema=CALL_SCHEMA)
    )
    df.select(
        "SOURCE_FILE",
        "CUSTOMER_ID",
        "REPRESENTATIVE_ID",
        "CATEGORY",
        "CALL_TYPE",
        "SENTIMENT",
        "SUMMARY",
        "CONFIDENCE",
    ).show(20, max_width=200)

    print("\n  Q: Which calls were escalated or left unresolved?")
    df.ai.complete(
        prompt="Category: {cat}. Summary: {summ}. "
        "Was this call escalated or unresolved? One sentence.",
        input_columns={"cat": col("CATEGORY"), "summ": col("SUMMARY")},
        model=MODEL,
        output_column="escalated",
    ).select("SOURCE_FILE", "CALL_TYPE", "SENTIMENT", "escalated").show(
        20, max_width=200
    )


# ===========================================================================

if __name__ == "__main__":
    session = make_session()
    try:
        example1_call_metadata(session)
        example2_summaries_and_qa(session)
    finally:
        session.close()
