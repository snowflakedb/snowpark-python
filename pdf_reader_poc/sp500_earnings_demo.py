#!/usr/bin/env python3
"""Aggressive demo: extract key financials from real S&P 500 earnings press
releases via session.read.pdf().

Pipeline:
  1. Resolve tickers (a fixed sample, or the full S&P 500 via --sp500) to CIKs.
  2. For each, find the last N earnings 8-Ks (form 8-K, item 2.02) on SEC EDGAR,
     download the press-release exhibit (HTML), render to a text-layer PDF.
     Sourced PDFs are cached on disk (--out-dir) so the run is resumable.
  3. Upload all PDFs to a prod SSE temp stage.
  4. session.read.pdf(schema=EARNINGS_SCHEMA, mode="text") -> typed table.

Run (sample):   python sp500_earnings_demo.py --connection prod3
Run (full 500): python sp500_earnings_demo.py --connection prod3 --sp500
"""

import argparse
import io
import json
import os
import re
import time
import traceback
import urllib.request

from bs4 import BeautifulSoup
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate
from xml.sax.saxutils import escape

from snowflake.snowpark import Session
from snowflake.snowpark.types import (
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
)

SAMPLE_TICKERS = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL",
                  "META", "JPM", "WMT", "XOM", "JNJ"]
QUARTERS = 4
STAGE = "SP500_EARNINGS_STAGE"
UA = {"User-Agent": "snowpark-read-pdf-poc shixuan.fan@snowflake.com"}

EARNINGS_SCHEMA = StructType([
    StructField("company_name", StringType()),
    StructField("fiscal_period", StringType()),      # e.g. "Q2 FY2026"
    StructField("period_end_date", DateType()),
    StructField("total_revenue", StringType()),      # as reported (units vary)
    StructField("net_income", StringType()),
    StructField("diluted_eps", DecimalType(10, 2)),
])


def _get(url: str, tries: int = 3) -> bytes:
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers=UA)
            return urllib.request.urlopen(req, timeout=30).read()
        except Exception:
            if i == tries - 1:
                raise
            time.sleep(1.5)
    raise RuntimeError("unreachable")


def sp500_tickers() -> list:
    """Fetch current S&P 500 constituents from Wikipedia."""
    html = _get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies").decode("utf-8", "ignore")
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", {"id": "constituents"}) or soup.find("table")
    tickers = []
    for tr in table.find_all("tr")[1:]:
        cell = tr.find(["td", "th"])
        if cell:
            tickers.append(cell.get_text(strip=True))
    return tickers


def ticker_to_cik() -> dict:
    data = json.loads(_get("https://www.sec.gov/files/company_tickers.json"))
    return {v["ticker"]: str(v["cik_str"]).zfill(10) for v in data.values()}


def resolve_cik(cik_map: dict, ticker: str):
    """SEC tickers vary in punctuation (BRK.B vs BRK-B); try variants."""
    for t in (ticker, ticker.replace(".", "-"), ticker.replace(".", ""),
              ticker.replace("-", ".")):
        if t in cik_map:
            return cik_map[t]
    return None


def last_earnings_8ks(cik: str, n: int):
    sub = json.loads(_get(f"https://data.sec.gov/submissions/CIK{cik}.json"))
    r = sub["filings"]["recent"]
    out = []
    for form, date, items, acc in zip(
        r["form"], r["filingDate"], r["items"], r["accessionNumber"]
    ):
        if form == "8-K" and "2.02" in items:
            out.append((date, acc))
        if len(out) >= n:
            break
    return out


def find_ex991(cik: str, acc: str):
    """Largest .htm exhibit (excluding cover / R\\d+ / index), press-release-boosted."""
    accnd = acc.replace("-", "")
    base = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accnd}"
    idx = json.loads(_get(f"{base}/index.json"))
    cands = []
    for i in idx["directory"]["item"]:
        n = i["name"]
        low = n.lower()
        if not low.endswith((".htm", ".html")):
            continue
        if "index" in low or re.match(r"r\d+\.htm", low):
            continue
        size = int(i.get("size") or 0)
        pref = 1 if re.search(r"ex.?-?99|exhibit99|press|earnings|fy\d+pr|q\d.*pr|_pr\b",
                              low) else 0
        cands.append((pref, size, n))
    if not cands:
        return None
    cands.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return f"{base}/{cands[0][2]}"


def html_to_pdf(html: str) -> bytes:
    soup = BeautifulSoup(html, "lxml")
    for t in soup(["script", "style"]):
        t.decompose()
    lines = []
    for tbl in soup.find_all("table"):
        for tr in tbl.find_all("tr"):
            cells = [re.sub(r"\s+", " ", c.get_text(" ", strip=True))
                     for c in tr.find_all(["td", "th"])]
            cells = [c for c in cells if c]
            if cells:
                lines.append(" | ".join(cells))
        tbl.decompose()
        lines.append("")
    for el in soup.find_all(["p", "div", "h1", "h2", "h3", "li"]):
        tx = re.sub(r"\s+", " ", el.get_text(" ", strip=True))
        if tx:
            lines.append(tx)
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter)
    style = getSampleStyleSheet()["BodyText"]
    style.fontSize = 8
    style.leading = 10
    doc.build([Paragraph(escape(l), style) for l in lines if l.strip()])
    return buf.getvalue()


def source_pdfs(out_dir: str, tickers: list) -> tuple:
    """Robustly source earnings PDFs to out_dir. Skips already-downloaded files.

    Returns (num_pdfs, num_companies_with_data, num_failed_companies).
    """
    os.makedirs(out_dir, exist_ok=True)
    cik_map = ticker_to_cik()
    total, with_data, failed = 0, 0, 0
    for n_done, tkr in enumerate(tickers, 1):
        try:
            cik = resolve_cik(cik_map, tkr)
            if not cik:
                continue
            got = 0
            for date, acc in last_earnings_8ks(cik, QUARTERS):
                path = os.path.join(out_dir, f"{tkr.replace('.', '_')}_{date}.pdf")
                if os.path.exists(path):
                    got += 1
                    total += 1
                    continue
                try:
                    time.sleep(0.12)  # SEC rate-limit courtesy
                    url = find_ex991(cik, acc)
                    if not url:
                        continue
                    time.sleep(0.12)
                    html = _get(url).decode("utf-8", "ignore")
                    pdf = html_to_pdf(html)
                    with open(path, "wb") as f:
                        f.write(pdf)
                    got += 1
                    total += 1
                except Exception as e:
                    print(f"  [{tkr}] filing {acc} failed: {type(e).__name__}: {e}")
            if got:
                with_data += 1
            if n_done % 25 == 0:
                print(f"  ...{n_done}/{len(tickers)} companies, {total} PDFs so far")
        except Exception as e:
            failed += 1
            print(f"  [{tkr}] FAILED: {type(e).__name__}: {e}")
    return total, with_data, failed


def build_session(conn: str) -> Session:
    sp = {"session_parameters": {"PYTHON_CONNECTOR_QUERY_RESULT_FORMAT": "JSON"},
          "client_session_keep_alive": True}
    return Session.builder.config("connection_name", conn).configs(sp).create()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--connection", default="prod3")
    ap.add_argument("--mode", default="text", choices=["text", "parse", "auto"])
    ap.add_argument("--model", default="llama3.1-70b")
    ap.add_argument("--sp500", action="store_true", help="use the full S&P 500")
    ap.add_argument("--limit", type=int, default=0, help="cap #companies (testing)")
    ap.add_argument("--out-dir", default="./sp500_earnings_pdfs")
    ap.add_argument("--target", default="TESTDB.PUBLIC.SP500_EARNINGS_FACTS")
    args = ap.parse_args()

    tickers = sp500_tickers() if args.sp500 else SAMPLE_TICKERS
    if args.limit:
        tickers = tickers[:args.limit]
    print(f"{len(tickers)} tickers. Sourcing earnings press releases -> {args.out_dir}")
    total, with_data, failed = source_pdfs(args.out_dir, tickers)
    print(f"Sourced {total} PDFs from {with_data} companies ({failed} failed).")

    session = build_session(args.connection)
    try:
        print(f"Uploading {total} PDFs to @{STAGE} ...")
        session.sql(
            f"CREATE OR REPLACE TEMPORARY STAGE {STAGE} "
            f"DIRECTORY=(ENABLE=TRUE) ENCRYPTION=(TYPE='SNOWFLAKE_SSE')"
        ).collect()
        files = [f for f in os.listdir(args.out_dir) if f.endswith(".pdf")]
        for i, fn in enumerate(sorted(files), 1):
            session.file.put(os.path.join(args.out_dir, fn), f"@{STAGE}",
                             auto_compress=False, overwrite=True)
            if i % 200 == 0:
                print(f"  uploaded {i}/{len(files)}")
        session.sql(f"ALTER STAGE {STAGE} REFRESH").collect()

        print(f"Running session.read.pdf(mode={args.mode!r}) -> {args.target} ...")
        session.read.pdf(f"@{STAGE}/", EARNINGS_SCHEMA, mode=args.mode,
                         model=args.model, target=args.target)

        t = session.table(args.target)
        n = t.count()
        n_ok = t.where("diluted_eps is not null or total_revenue is not null").count()
        print(f"\n=== {args.target}: {n} rows, {n_ok} with extracted financials "
              f"({100*n_ok//max(n,1)}%) ===")
        for r in t.sort("file_url").limit(20).collect():
            tag = r["FILE_URL"].split("/")[-1].replace("%2e", ".")
            print(f"{tag:24} | {str(r['COMPANY_NAME'])[:24]:24} | "
                  f"end={r['PERIOD_END_DATE']} | rev={str(r['TOTAL_REVENUE'])[:14]:14} | "
                  f"eps={r['DILUTED_EPS']}")
        print(f"\nFull results: SELECT * FROM {args.target};")
    finally:
        session.close()


if __name__ == "__main__":
    main()
