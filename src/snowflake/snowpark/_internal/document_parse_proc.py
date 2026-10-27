#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Stored-procedure handler for parsing documents to Markdown with AI_PARSE_DOCUMENT,
then improving each page with AI_COMPLETE where the parser is known to be weak.

Where to start reading: ``run`` at the bottom of the file is the entry point. It runs
AI_PARSE_DOCUMENT on every file, decides per page which follow-up calls the page needs
(``plan_page``), runs them all at once in rounds (``DocumentParse.run_rounds``) and
assembles each page from the answers (``assemble_page``).

Every page is routed by what it contains, never by what kind of document it is:

- A page with no PDF text layer (a scan) is transcribed whole by AI_COMPLETE.
- Any other page keeps the parser's text, with inline styling re-applied from the PDF's
  text layer (``enrich_markdown``) and its formatting corrected by AI_COMPLETE against
  the page (text repair).
- Its tables are rebuilt from the parser's pipe tables (``table_to_html``, which restores
  multi-row headers and merged cells) and corrected by AI_COMPLETE against the page
  (table repair). A table too large for one answer is transcribed in row ranges.
- Every image the parser extracted is sent to AI_COMPLETE, which transcribes a chart as
  an HTML table and describes any other image in one line.

A follow-up call that fails or returns nothing is retried once, and otherwise the page
keeps the parser's own output for that part, so a page is never worse than plain
AI_PARSE_DOCUMENT apart from the cost of the calls.

The pipeline follows the ParseBench research in snowflake-eng/snowflake#548424. Uploaded
and registered as a standalone file, so it imports nothing from snowpark's internals.
Every function except ``run`` and ``DocumentParse`` is pure and unit-tested directly.
"""

import base64
import hashlib
import html
import io
import json
import os
import re
import statistics
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from snowflake.snowpark import DataFrame, Session

PAGE_SEPARATOR = "\n\n"

# ---------------------------------------------------------------------------
# Pipe tables to HTML. AI_PARSE_DOCUMENT writes tables as Markdown pipe tables, which can
# hold only one header row and no merged cells; these rules rebuild that structure from
# the table's shape alone (which cells are empty and where), never from its words.
# ---------------------------------------------------------------------------

_SEPARATOR = re.compile(r"^\s*\|?\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)*\|?\s*$")
_NUMBER = re.compile(r"^[\s$€£¥%()+\-–.,:/0-9]*[0-9][\s$€£¥%()+\-–.,:/0-9a-zA-Z]{0,3}$")
_MAX_EXTRA_HEADER_ROWS = 3
_ALLOWED_INLINE_TAGS = ("br", "sup", "sub", "b", "i", "strong", "em")
_LATEX_SYMBOLS = {
    "alpha": "α",
    "beta": "β",
    "gamma": "γ",
    "delta": "δ",
    "epsilon": "ε",
    "theta": "θ",
    "lambda": "λ",
    "mu": "µ",
    "pi": "π",
    "sigma": "σ",
    "tau": "τ",
    "phi": "φ",
    "omega": "ω",
    "Delta": "Δ",
    "Sigma": "Σ",
    "Omega": "Ω",
    "circ": "°",
    "degree": "°",
    "leq": "≤",
    "le": "≤",
    "geq": "≥",
    "ge": "≥",
    "neq": "≠",
    "pm": "±",
    "times": "×",
    "div": "÷",
    "cdot": "·",
    "approx": "≈",
    "infty": "∞",
    "rightarrow": "→",
    "to": "→",
    "leftarrow": "←",
    "%": "%",
    "$": "$",
    "&": "&",
    "_": "_",
    "#": "#",
    "bullet": "•",
    "prime": "′",
    "sim": "~",
}


def split_row(line: str) -> List[str]:
    """The stripped cells of one pipe-table line; an escaped ``\\|`` stays in its cell."""
    s = line.strip()
    if s.startswith("|"):
        s = s[1:]
    if s.endswith("|"):
        s = s[:-1]
    return [p.replace("\\|", "|").strip() for p in re.split(r"(?<!\\)\|", s)]


def find_tables(lines: List[str]) -> List[Tuple[int, int]]:
    """(start, end) line ranges of the pipe tables in ``lines``: a row line, a separator
    line, then every following row line."""
    spans, i = [], 0
    while i < len(lines) - 1:
        if lines[i].lstrip().startswith("|") and _SEPARATOR.match(lines[i + 1]):
            j = i + 2
            while j < len(lines) and lines[j].lstrip().startswith("|"):
                j += 1
            spans.append((i, j))
            i = j
        else:
            i += 1
    return spans


def _is_blank(cell: str) -> bool:
    return cell.strip() == ""


def _looks_numeric(cell: str) -> bool:
    """True for values like '1,240', '$12.4M', '(11)%', '-4.9%' or '12:04'."""
    return bool(_NUMBER.match(cell.strip()))


def _is_title_band(row: List[str]) -> bool:
    """A row whose only filled cell is the first: a table title printed above it."""
    return len(row) > 1 and not _is_blank(row[0]) and all(_is_blank(c) for c in row[1:])


def _promote_header_rows(
    header: List[str], body: List[List[str]]
) -> Tuple[List[List[str]], List[List[str]]]:
    """Move header rows that the one-row pipe header pushed into the body back up: rows
    after a title band; a fully filled, non-numeric row under a header with gaps; and
    sub-header rows (empty first cell, something else filled), at most three."""
    if _is_title_band(header) and body:
        rest_head, rest_body = _promote_header_rows(body[0], body[1:])
        return [header] + rest_head, rest_body
    head = [header]
    k = 0
    if (
        body
        and any(_is_blank(c) for c in header)
        and all(not _is_blank(c) for c in body[0])
        and not any(_looks_numeric(c) for c in body[0])
    ):
        head.append(body[0])
        k = 1
    while (
        k < len(body)
        and len(head) - 1 < _MAX_EXTRA_HEADER_ROWS
        and _is_blank(body[k][0])
        and any(not _is_blank(c) for c in body[k][1:])
    ):
        head.append(body[k])
        k += 1
    return head, body[k:]


def _normalize_inline(text: str) -> str:
    """Markdown and LaTeX markup inside a cell, which HTML would show literally, turned
    into HTML tags or plain characters."""
    t = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)
    t = re.sub(
        r"\\(?:mathrm|text|mathsf|mathbf|mathit|operatorname|textbf|textit)\s*\{([^{}]*)\}",
        r"\1",
        t,
    )
    t = re.sub(r"\^\{([^{}]*)\}", r"<sup>\1</sup>", t)
    t = re.sub(r"_\{([^{}]*)\}", r"<sub>\1</sub>", t)
    t = re.sub(r"\\(left|right)\s*", "", t)
    t = re.sub(
        r"\\([A-Za-z]+|[%$&_#])",
        lambda m: _LATEX_SYMBOLS.get(m.group(1), m.group(0)),
        t,
    )
    if "\\" in text or "^{" in text or "_{" in text:
        t = re.sub(r"\$([^$]*)\$", r"\1", t)
    return t


def _cell_html(text: str) -> str:
    """A cell's text, HTML-escaped except for a few inline tags the parser writes."""
    out = html.escape(_normalize_inline(text), quote=False)
    tags = "|".join(_ALLOWED_INLINE_TAGS)
    return re.sub(rf"&lt;(/?(?:{tags})\s*/?)&gt;", r"<\1>", out, flags=re.I)


def _header_html(head: List[List[str]]) -> str:
    """Header rows as HTML: a title band spans the whole width, a label followed by
    empty cells spans them (never from the first column), and a label with only empty
    cells below it spans down over them."""
    width = len(head[0])
    first = 0
    while first < len(head) - 1 and _is_title_band(head[first]):
        first += 1
    n = len(head)
    covered = [[False] * width for _ in range(n)]
    out = []
    for ri, row in enumerate(head):
        if ri < first:
            out.append(f'<tr><th colspan="{width}">{_cell_html(row[0])}</th></tr>')
            continue
        cells, c = [], 0
        while c < width:
            if covered[ri][c]:
                c += 1
                continue
            span = 1
            if c > 0 and not _is_blank(row[c]):
                while (
                    c + span < width
                    and _is_blank(row[c + span])
                    and not covered[ri][c + span]
                ):
                    span += 1
            rowspan = 1
            if (
                span == 1
                and not _is_blank(row[c])
                and ri < n - 1
                and all(_is_blank(head[k][c]) for k in range(ri + 1, n))
            ):
                rowspan = n - ri
                for k in range(ri + 1, n):
                    covered[k][c] = True
            attr = (f' colspan="{span}"' if span > 1 else "") + (
                f' rowspan="{rowspan}"' if rowspan > 1 else ""
            )
            cells.append(f"<th{attr}>{_cell_html(row[c])}</th>")
            c += span
        out.append("<tr>" + "".join(cells) + "</tr>")
    return "\n".join(out)


def _render(head: List[List[str]], body: List[List[str]]) -> str:
    parts = ["<table>", _header_html(head)]
    for r in body:
        parts.append("<tr>" + "".join(f"<td>{_cell_html(c)}</td>" for c in r) + "</tr>")
    parts.append("</table>")
    return "\n".join(parts)


def _repeated_block_width(labels: List[str]) -> Optional[int]:
    """Width of the label block when the header row is one block of at least two labels
    printed k >= 2 times side by side; None otherwise."""
    n = len(labels)
    norm = [label.strip().lower() for label in labels]
    for w in range(2, n // 2 + 1):
        if (
            n % w == 0
            and any(norm[:w])
            and all(norm[i] == norm[i % w] for i in range(n))
        ):
            return w
    return None


def _render_blocks(head: List[List[str]], body: List[List[str]]) -> str:
    """Render the table, as one table per block when its columns repeat side by side."""
    blocks = _repeated_block_width(head[-1])
    if not blocks:
        return _render(head, body)
    return "\n".join(
        _render(
            [r[b : b + blocks] for r in head],
            [
                r[b : b + blocks]
                for r in body
                if any(not _is_blank(c) for c in r[b : b + blocks])
            ],
        )
        for b in range(0, len(head[-1]), blocks)
    )


def rows_to_html(header: List[str], body: List[List[str]]) -> str:
    """One parsed pipe table as HTML, split where a body row repeats the column-header
    row (several tables returned as one) or where columns repeat side by side."""
    width = max([len(header)] + [len(r) for r in body])
    header = header + [""] * (width - len(header))
    body = [r + [""] * (width - len(r)) for r in body]
    head, body = _promote_header_rows(header, body)
    if len(head[-1]) > 1:
        key = [c.strip().lower() for c in head[-1]]
        for i in range(1, len(body)):
            if [c.strip().lower() for c in body[i]] == key and any(key[1:]):
                cut = i - 1 if _is_title_band(body[i - 1]) else i
                if cut == 0:
                    continue
                return (
                    _render_blocks(head, body[:cut])
                    + "\n"
                    + rows_to_html(body[cut], body[cut + 1 :])
                )
    return _render_blocks(head, body)


def table_to_html(lines: List[str]) -> str:
    """One pipe table (header line, separator line, body lines) as HTML."""
    return rows_to_html(split_row(lines[0]), [split_row(line) for line in lines[2:]])


_HTML_TABLE = re.compile(r"<table\b.*?</table\s*>", re.S | re.I)


def replace_tables(md: str) -> Tuple[str, List[str]]:
    """``md`` with each table replaced by a ``[[TABLE n]]`` marker line, and the tables
    themselves as HTML, in order. Pipe tables are converted; HTML tables, which the
    parser sometimes writes instead, are kept as they are."""
    tables: List[str] = []

    def pipe_tables(segment: str) -> str:
        lines = segment.split("\n")
        out, last = [], 0
        for start, end in find_tables(lines):
            out.extend(lines[last:start])
            tables.append(table_to_html(lines[start:end]))
            out.append(table_marker(len(tables)))
            last = end
        out.extend(lines[last:])
        return "\n".join(out)

    parts, pos = [], 0
    for m in _HTML_TABLE.finditer(md):
        parts.append(pipe_tables(md[pos : m.start()]))
        tables.append(m.group(0))
        parts.append("\n" + table_marker(len(tables)) + "\n")
        pos = m.end()
    parts.append(pipe_tables(md[pos:]))
    return "".join(parts), tables


def table_marker(n: int) -> str:
    return f"[[TABLE {n}]]"


def degeneracy(md: str, min_cells: int = 20) -> float:
    """0..1: how much the pipe tables in ``md`` look invented rather than read: the
    share of body cells (first column excluded) holding the single most frequent value,
    or the share of body rows repeating an earlier row, whichever is worst. Tables with
    fewer than ``min_cells`` filled cells are ignored."""
    lines = md.splitlines()
    worst = 0.0
    for start, end in find_tables(lines):
        rows = [split_row(line) for line in lines[start + 2 : end]]
        cells = [c for r in rows for c in r[1:] if c.strip()]
        if len(cells) < min_cells:
            continue
        top_value_share = max(cells.count(v) for v in set(cells)) / len(cells)
        duplicate_row_share = 1 - len({tuple(r) for r in rows}) / len(rows)
        worst = max(worst, top_value_share, duplicate_row_share)
    return worst


# ---------------------------------------------------------------------------
# Inline styling from the PDF text layer. AI_PARSE_DOCUMENT rarely writes bold and never
# italic or superscript, but a digital PDF's fonts and glyph positions still carry them.
# ---------------------------------------------------------------------------

_BOLD = re.compile(r"bold|black|heavy|semibold|demi", re.I)
_ITALIC = re.compile(r"italic|oblique", re.I)
_WRAP = {
    "b": ("**", "**"),
    "i": ("*", "*"),
    "bi": ("***", "***"),
    "sup": ("<sup>", "</sup>"),
}
_SUP_SIZE_RATIO = 0.8
_MIN_SPAN_CHARS = 3
_HEADING_SIZE_RATIO = 1.15
_HEADING_MAX_WORDS = 15
_MAX_HEADING_LEVELS = 4


def _norm_line(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[*_#]", "", text)).strip().lower()


def styled_spans(layout: Any) -> List[Tuple[str, str]]:
    """(style, text) runs from one pdfminer page layout, style one of 'b', 'i', 'bi' and
    'sup'; a styled phrase that wraps onto the next line of its text box is one run."""
    from pdfminer.layout import LTChar, LTTextContainer, LTTextLine

    spans: List[Tuple[str, str]] = []
    for box in layout:
        if not isinstance(box, LTTextContainer):
            continue
        box_start = len(spans)
        for line in box:
            if not isinstance(line, LTTextLine):
                continue
            chars = [c for c in line if isinstance(c, LTChar)]
            if not chars:
                continue
            med = statistics.median(c.size for c in chars)
            base = statistics.median(c.y0 for c in chars)
            cur_style, cur = None, []
            for c in line:
                if not isinstance(c, LTChar):  # an inter-word space or line break
                    cur.append(" ")
                    continue
                if c.size < _SUP_SIZE_RATIO * med and c.y0 > base + 0.15 * med:
                    style = "sup"
                else:
                    b = bool(_BOLD.search(c.fontname or ""))
                    i = bool(_ITALIC.search(c.fontname or ""))
                    style = ("b" if b else "") + ("i" if i else "") or None
                if style != cur_style:
                    if cur_style and "".join(cur).strip():
                        spans.append((cur_style, "".join(cur).strip()))
                    cur_style, cur = style, []
                cur.append(c.get_text())
            if cur_style and "".join(cur).strip():
                spans.append((cur_style, "".join(cur).strip()))
        merged: List[Tuple[str, str]] = []
        for style, text in spans[box_start:]:
            if merged and merged[-1][0] == style and style != "sup":
                merged[-1] = (style, merged[-1][1] + " " + text)
            else:
                merged.append((style, text))
        spans[box_start:] = merged
    return spans


def heading_levels(layout: Any) -> Dict[str, int]:
    """{normalized line text: heading level} for one pdfminer page layout: lines set at
    least 1.15x larger than the page's most common glyph size, with at most 15 words,
    ranked by size into levels 1-4."""
    from pdfminer.layout import LTChar, LTTextContainer, LTTextLine

    lines: List[Tuple[str, float]] = []
    sizes: Counter = Counter()
    for box in layout:
        if not isinstance(box, LTTextContainer):
            continue
        for line in box:
            if not isinstance(line, LTTextLine):
                continue
            chars = [c for c in line if isinstance(c, LTChar) and c.get_text().strip()]
            if not chars:
                continue
            for c in chars:
                sizes[round(c.size * 2) / 2] += 1
            lines.append(
                (_norm_line(line.get_text()), statistics.median(c.size for c in chars))
            )
    if not sizes:
        return {}
    body = sizes.most_common(1)[0][0]
    big = [
        (t, round(sz * 2) / 2)
        for t, sz in lines
        if sz >= _HEADING_SIZE_RATIO * body and 0 < len(t.split()) <= _HEADING_MAX_WORDS
    ]
    ranks = sorted({sz for _, sz in big}, reverse=True)[:_MAX_HEADING_LEVELS]
    return {t: ranks.index(sz) + 1 for t, sz in big if sz in ranks}


def apply_headings(md: str, levels: Dict[str, int]) -> str:
    """``md`` with every line whose text is a known heading written at that level."""
    out = []
    for line in md.splitlines():
        key = _norm_line(line.lstrip("#"))
        if key in levels and not line.lstrip().startswith("|"):
            line = "#" * levels[key] + " " + line.lstrip("#").strip().strip("*").strip()
        out.append(line)
    return "\n".join(out)


def apply_spans(md: str, spans: List[Tuple[str, str]]) -> str:
    """``md`` with each styled run wrapped in its markup, where the run's text occurs
    exactly once, is not a whole heading line and is not already inside markup; runs
    under three characters (superscripts excepted) are skipped as ambiguous."""
    heading_lines = {
        line.lstrip("#").strip()
        for line in md.splitlines()
        if line.lstrip().startswith("#")
    }
    for style, text in spans:
        text = re.sub(r"\s+", " ", text)
        if style != "sup" and len(text) < _MIN_SPAN_CHARS:
            continue
        if text in heading_lines:
            continue
        if style == "sup":
            pat = re.compile(r"(?<=[\w)\]])" + re.escape(text) + r"(?![\w<])")
        else:
            words = r"\s+".join(re.escape(w) for w in text.split())
            pat = re.compile(r"(?<![\w*])" + words + r"(?![\w*])")
        hits = list(pat.finditer(md))
        if len(hits) != 1:
            continue
        m = hits[0]
        before = md[max(0, m.start() - 6) : m.start()]
        if "<sup>" in before or "**" in before[-2:]:
            continue
        a, b = _WRAP[style]
        md = md[: m.start()] + a + m.group(0) + b + md[m.end() :]
    return md


# ---------------------------------------------------------------------------
# Prompts. Each is sent as AI_COMPLETE's predicate with the page (or image) as its FILE,
# so the text is inserted as is: no PROMPT() template, no brace escaping.
# ---------------------------------------------------------------------------

TRANSCRIBE_PROMPT = """Convert this document page to Markdown. Transcribe every piece of text exactly as written, in natural reading order. Do not summarize, correct, or add anything.
Formatting rules:
- Use # / ## / ### headings for titles and section headings.
- Write every table as HTML (<table>, <tr>, <th>, <td>), never as Markdown pipe tables. Use colspan/rowspan for merged cells. Keep exactly the columns shown on the page; do not split or merge columns. Use <br/> for line breaks inside a cell and <strong>...</strong> for bold text inside cells.
- Outside tables, preserve bold (**text**), italics (*text*), strikethrough (~~text~~), and superscript/subscript (<sup>, <sub>).
- Skip running page headers/footers and page numbers only if they are repeated boilerplate; otherwise include them.
Output only the Markdown, with no preamble and no code fences."""

REPAIR_TEXT_PROMPT = """You are checking a document parser's Markdown for this page. Its words are usually right, but formatting is often missing or wrong.

Return the same Markdown with the formatting corrected to match the page:
- Headings: mark every title and section heading with #, ##, ### according to its visual hierarchy on the page; do not mark ordinary text as a heading.
- Inline styling: **bold**, *italic*, <u>underline</u>, ~~strikethrough~~, <sup>superscript</sup>, <sub>subscript</sub>, ==highlight== wherever the page shows them.
- Math: write formulas in LaTeX between single dollar signs (inline) or double dollar signs (display).
- Keep the words, their order and any tables exactly as given, except to fix an obviously misread word. Do not add, drop or summarize content.
- Keep every [[TABLE n]] line and every image reference such as ![img-0.jpeg](img-0.jpeg) exactly as given, on its own line: they stand for content handled separately.
Output only the corrected Markdown, with no explanation and no code fences.

PARSER MARKDOWN:
<<TEXT>>"""

REPAIR_TABLES_PROMPT = """You are checking the tables a document parser extracted from this page. The parser's text is usually right, but its table structure is often wrong: multi-row headers flattened, merged (spanning) header cells lost, columns split or shifted, separate tables merged or one table split, and occasional misread cells.

Below are the parser's tables as HTML. Compare them with the page and return the page's tables, corrected:
- Keep every table on the page, in reading order, as HTML (<table>, <tr>, <th>, <td>). Use colspan/rowspan for merged cells, <th> for header cells, <br/> for line breaks inside a cell.
- Keep exactly the columns and rows shown on the page; do not split or merge columns.
- Every printed row is its own <tr>. When the lines of a cell line up with the lines of the cells beside it, they are separate rows: never stack several rows into one cell with <br/>.
- Fix misread cell text and numbers so they match the page exactly. Keep text that is already correct unchanged.
- Output only the corrected tables, with no explanation and no code fences.

PARSER TABLES:
<<TABLES>>"""

CHART_CONTEXT_PROMPT = """This image was cropped from a document page. Text that a parser extracted from the same page is given below as CONTEXT; the chart's legend, axis titles, units and caption are often there rather than inside the crop.

If the image is a chart or graph (bar, line, pie, scatter, area, etc.), transcribe the data it shows as one HTML table:
- First row: <th> header cells. The first header cell names the category axis. The other header cells are the series names exactly as written in the chart's legend; if the legend is not visible in the image, take the series names from the CONTEXT. Never invent generic names like "Series 1" or "Value" when a real name is available, and never add how a series is drawn (such as "(bar)", "(line)" or "(diamond)") to its name.
- Include every series shown: every bar colour, line, marker or dot type is its own column.
- One row per category (x-axis label, pie slice, bar label), with the category label exactly as written.
- Cells contain the numeric values. Use the printed data labels when present, copied exactly with their units (%, $). Otherwise read the value off the axis as precisely as you can.
- Put the chart title, if any, on a line above the table.
If the image is not a chart (photo, logo, diagram, map without data), output one short line describing it and no table.
Output only the table (and title line), with no preamble and no code fences.

CONTEXT:
<<CONTEXT>>"""

CHART_PROMPT = """This image comes from a document page. If it is a chart or graph (bar, line, pie, scatter, area, etc.), transcribe the data it shows as one HTML table:
- First row: <th> header cells. The first header cell names the category axis; the others are the series names exactly as written in the legend (use the value-axis title if there is only one series), without adding how a series is drawn (such as "(bar)" or "(line)").
- One row per category (x-axis label, pie slice, bar label), with the category label exactly as written in the first cell.
- Cells contain the numeric values. Use the printed data labels when present, copied exactly. Otherwise read the value off the axis as precisely as you can.
- Put the chart title, if any, on a line above the table.
If the image is not a chart (photo, logo, diagram, map without data), output one short line describing it and no table.
Output only the table (and title line), with no preamble and no code fences."""

ROWS_FIRST_PROMPT = """Transcribe every table on this page as HTML (<table>, <tr>, <th>, <td>; colspan/rowspan for merged cells; <br/> inside cells). Copy every cell exactly as printed.

The tables are too long for one answer, so work in parts. In this part, output the header rows and at most <<ROWS>> body rows in total, counting across all tables, in reading order. Stop only after a complete </tr>.
- If rows remain after this part, do NOT close the current table. End your answer with a final line containing only MORE.
- If you have transcribed every row of every table, close the last table and end with a final line containing only DONE.
Output only HTML and that final line, with no explanation and no code fences."""

ROWS_NEXT_PROMPT = """You are continuing a transcription of the tables on this page as HTML, which is being done in parts.

The table currently open has these header rows:
<<HEADER>>

The last row already transcribed is:
<<LAST>>

Continue from the row immediately after that one. Output at most <<ROWS>> more body rows as <tr> elements (no <table> tag and no header for the open table). If the open table ends, output </table>, then any following table in full (<table>, its header rows, its rows), still counting toward the <<ROWS>> rows.
- If rows remain after this part, do not close the current table and end with a final line containing only MORE.
- If you have now transcribed every row of every table, close the last table and end with a final line containing only DONE.
Copy every cell exactly as printed. Output only HTML and that final line, with no explanation and no code fences."""

PLACEHOLDER = re.compile(r"!\[([^\]]*)\]\(([^)]+)\)")
_FENCE = re.compile(r"^\s*```[a-zA-Z]*\s*\n(.*?)\n?```\s*$", re.S)


def strip_fences(text: str) -> str:
    """An answer without the code fence a model sometimes wraps it in."""
    m = _FENCE.match(text)
    return m.group(1) if m else text


def chart_context(content: str, max_chars: int) -> str:
    """The page text sent with each of its images: placeholders blanked, truncated."""
    return PLACEHOLDER.sub("[image]", content)[:max_chars]


def open_table_context(text: str) -> Tuple[str, str]:
    """For a row-range transcription in progress: the header rows of the last table
    opened, and the last row written."""
    start = text.rfind("<table")
    table = text[start:] if start >= 0 else text
    rows = re.findall(r"<tr\b.*?</tr>", table, re.S)
    header = [r for r in rows if "<th" in r and "<td" not in r]
    return "\n".join(header) or "(none)", (rows[-1] if rows else "") or "(none)"


def split_marker(text: str) -> Tuple[str, str]:
    """(body, marker) of a row-range answer whose last line is MORE or DONE; marker is
    empty when the answer has neither."""
    lines = text.rstrip().splitlines()
    marker = lines[-1].strip() if lines else ""
    if marker in ("MORE", "DONE"):
        return "\n".join(lines[:-1]).strip(), marker
    return text.strip(), ""


def close_tables(text: str) -> str:
    """``text`` with a closing tag for every table it left open."""
    return text + "\n</table>" * max(0, text.count("<table") - text.count("</table>"))


# ---------------------------------------------------------------------------
# One page: what to ask for, and how to put the answers together.
# ---------------------------------------------------------------------------


@dataclass
class PageState:
    """One page of one file: the parser's output and every follow-up answer."""

    file: str
    index: int  # 0-based page position in its file
    content: str  # the parser's Markdown for the page
    images: List[Dict[str, Any]]  # {"id": ..., "path": staged image path}
    staged: Optional[str] = None  # the page as its own file, for page-level calls
    words: int = 0  # words in the PDF text layer
    layer_chars: int = 0  # non-space characters in the PDF text layer
    text: Optional[str] = None  # the parser's text with tables replaced by markers
    tables: List[str] = field(default_factory=list)  # parser tables as HTML
    enriched: Optional[str] = None  # ``text`` with styling from the text layer
    transcription: Optional[str] = None
    repaired_text: Optional[str] = None
    repaired_tables: Optional[str] = None
    row_parts: List[str] = field(default_factory=list)
    charts: Dict[str, str] = field(default_factory=dict)  # image id -> answer
    trace: Dict[str, Any] = field(default_factory=dict)  # route and call outcomes


def is_scan(words: int, settings: Dict[str, Any]) -> bool:
    """A page whose text layer has too few words to be digital: scanned or handwritten."""
    return words < settings["scan_min_words"]


def visible_chars(text: str) -> int:
    """Characters of ``text`` that carry content: no whitespace, no Markdown or table
    syntax, no image references."""
    return len(re.sub(r"[\s|#*_\-:`]", "", PLACEHOLDER.sub("", text)))


def coverage(page: PageState) -> float:
    """How much of the page's PDF text layer the parser returned (by visible characters);
    1.0 when the page has no text layer to compare with."""
    if not page.layer_chars:
        return 1.0
    return visible_chars(page.content) / page.layer_chars


def missed_content(page: PageState, settings: Dict[str, Any]) -> bool:
    """The parser returned well under the page's own text: it dropped part of the page
    (typically a large table), so the page is transcribed whole instead. A page with
    images gets a much lower limit: its missing text is usually chart labels, which the
    chart step recovers, unless the parser returned almost nothing."""
    limit = (
        settings["coverage_min_with_images"]
        if page.images
        else settings["coverage_min"]
    )
    return page.layer_chars >= settings["coverage_min_chars"] and coverage(page) < limit


def estimated_page_tokens(page: PageState, settings: Dict[str, Any]) -> int:
    """Rough output tokens of the whole page transcribed, from its text-layer words."""
    return settings["tokens_per_word"] * page.words


def uses_row_ranges(page: PageState, settings: Dict[str, Any]) -> bool:
    """A page to transcribe whole whose answer would not fit in one call."""
    return estimated_page_tokens(page, settings) > settings["row_range_tokens"]


def splice_tables(text: str, blocks: List[str]) -> str:
    """``text`` with marker n replaced by ``blocks[n-1]``. When there is one block for
    several markers (the repaired tables, which may split or merge them), it goes at the
    first marker and the others are dropped. Blocks whose marker is missing are appended."""
    markers = [table_marker(n) for n in range(1, len(blocks) + 1)]
    if len(blocks) == 1:
        markers = re.findall(r"\[\[TABLE \d+\]\]", text) or markers[:1]
        first = True
        for m in markers:
            if m in text:
                text = text.replace(m, blocks[0] if first else "", 1)
                if first:
                    first = False
        if first:
            text = text.rstrip() + "\n\n" + blocks[0]
        return re.sub(r"\n{3,}", "\n\n", text)
    for m, block in zip(markers, blocks):
        if m in text:
            text = text.replace(m, block, 1)
        else:
            text = text.rstrip() + "\n\n" + block
    return re.sub(r"\[\[TABLE \d+\]\]", "", text)


def splice_charts(text: str, charts: Dict[str, str]) -> str:
    """``text`` with each image placeholder replaced by its chart answer; answers whose
    placeholder is missing are appended."""
    used = set()

    def repl(m: re.Match) -> str:
        answer = charts.get(m.group(2))
        if not answer:
            return m.group(0)
        used.add(m.group(2))
        return "\n\n" + answer.strip() + "\n\n"

    text = PLACEHOLDER.sub(repl, text)
    extra = [a.strip() for k, a in charts.items() if k not in used and a]
    return text.rstrip() + "".join("\n\n" + a for a in extra)


def assemble_page(page: PageState, settings: Dict[str, Any]) -> str:
    """The page's final Markdown: the transcription for a scan; otherwise the repaired
    (or enriched) text with the best available tables and the chart answers spliced in."""
    if page.transcription:
        return page.transcription.strip()
    if page.row_parts:
        # A page transcribed in row ranges: its tables replace the parser's (which
        # missed most of them); the parser's other text stays around them.
        text = page.enriched or page.text or page.content
        return splice_tables(text, [close_tables("\n".join(page.row_parts))]).strip()
    text = page.repaired_text or page.enriched or page.text or page.content
    if page.repaired_tables:
        text = splice_tables(text, [page.repaired_tables])
    elif page.tables:
        text = splice_tables(text, page.tables)
    return splice_charts(text, page.charts).strip()


# ---------------------------------------------------------------------------
# Running it: AI_PARSE_DOCUMENT on every file, then rounds of AI_COMPLETE calls.
# ---------------------------------------------------------------------------

_PARSE_SQL = (
    "SELECT AI_PARSE_DOCUMENT(TO_FILE(?), "
    "{'mode': 'LAYOUT', 'page_split': true, 'extract_images': true}, TRUE)"
)
_PARSE_PLAIN_SQL = "SELECT AI_PARSE_DOCUMENT(TO_FILE(?), {'mode': 'LAYOUT'}, TRUE)"
_COMPLETE_SQL = (
    "SELECT AI_COMPLETE(model => ?, predicate => ?, file => TO_FILE(?), "
    "model_parameters => PARSE_JSON(?), return_error_details => TRUE)"
)


def qualify_listed_name(path: str, listed_name: str) -> str:
    """LIST returns '<stage name>/<relative path>' without database or schema; rebuild a
    reference that resolves regardless of the session's current schema."""
    stage = path.lstrip("@").split("/", 1)[0]
    relative = listed_name.split("/", 1)[1] if "/" in listed_name else listed_name
    return f"@{stage}/{relative}"


def read_answer(raw: Any) -> Tuple[Optional[str], Optional[str]]:
    """(text, error) from one ``AI_COMPLETE(..., return_error_details => TRUE)`` result;
    an empty answer is an error."""
    result = json.loads(raw) if isinstance(raw, str) else raw
    value, error = result.get("value"), result.get("error")
    if error is not None:
        return None, str(error)
    if not isinstance(value, str) or not value.strip():
        return None, "empty answer"
    return strip_fences(value.strip()), None


@dataclass
class Call:
    """One AI_COMPLETE call to make for a page."""

    kind: str  # transcribe | repair_text | repair_tables | rows | chart | chart_plain
    page: PageState
    prompt: str
    file: str
    attempt: int = 0
    image: Optional[str] = None  # image id, for chart calls
    max_tokens: Optional[int] = None  # None: the default max_tokens


class DocumentParse:  # pragma: no cover - exercised against a live account
    """The calls for every file under a path: parse each file, plan each page's follow-up
    calls, run them in rounds until none is left, and assemble each file's Markdown."""

    def __init__(
        self, session: Session, model: str, settings: Dict[str, Any], work_stage: str
    ) -> None:
        self.session = session
        self.model = model
        self.settings = settings
        self.work_stage = work_stage
        self.max_tokens = settings["max_tokens"]

    def put(self, data: bytes, path: str, name: str) -> str:
        """Stage ``data`` under this file's work folder; returns the staged path."""
        digest = hashlib.md5(path.encode()).hexdigest()[:12]
        target = f"{self.work_stage}/document_parse/{digest}/{name}"
        self.session.file.put_stream(
            io.BytesIO(data), target, auto_compress=False, overwrite=True
        )
        return target

    def pages_of(
        self, path: str, content: bytes, parsed: Dict[str, Any]
    ) -> List[PageState]:
        """Every page of one parsed PDF, with its images and its own one-page file staged
        and its text-layer word count measured."""
        from pdfminer.high_level import extract_text
        from pypdf import PdfReader, PdfWriter

        reader = PdfReader(io.BytesIO(content))
        stem = os.path.splitext(os.path.basename(path))[0]
        pages = []
        for entry in (parsed.get("value") or {}).get("pages") or []:
            index = int(entry.get("index", len(pages)))
            images = []
            for image in entry.get("images") or []:
                data = image.get("image_base64", "").split(",", 1)[-1]
                if data:
                    images.append(
                        {
                            "id": image["id"],
                            "path": self.put(
                                base64.b64decode(data),
                                path,
                                f"{stem}_p{index}_{image['id']}",
                            ),
                        }
                    )
            if len(reader.pages) == 1:
                staged = path
            else:
                writer = PdfWriter()
                writer.add_page(reader.pages[index])
                buffer = io.BytesIO()
                writer.write(buffer)
                staged = self.put(buffer.getvalue(), path, f"{stem}_p{index}.pdf")
            try:
                layer = extract_text(io.BytesIO(content), page_numbers=[index])
            except Exception:
                layer = ""
            pages.append(
                PageState(
                    path,
                    index,
                    entry.get("content") or "",
                    images,
                    staged,
                    len(layer.split()),
                    visible_chars(layer),
                )
            )
        return pages

    def enrich(self, page: PageState, content: bytes) -> str:
        """The page's marked-up text with styling re-applied from the PDF text layer."""
        from pdfminer.high_level import extract_pages

        try:
            layout = next(
                iter(extract_pages(io.BytesIO(content), page_numbers=[page.index]))
            )
            text = apply_headings(page.text, heading_levels(layout))
            return apply_spans(text, styled_spans(layout))
        except Exception:
            return page.text

    def plan_page(self, page: PageState) -> List[Call]:
        """The first calls a page needs. The page is transcribed whole when the parser
        returned nothing, when it is a scan, or when the parser missed much of its text
        (in row ranges if the transcription would not fit in one answer). Otherwise it
        gets text repair, table repair for its tables and one chart call per image."""
        s = self.settings
        page.trace.update(
            page=page.index,
            words=page.words,
            layer_chars=page.layer_chars,
            parser_chars=visible_chars(page.content),
            tables=len(page.tables),
            images=len(page.images),
        )
        transcribe_reason = None
        if not page.content.strip():
            transcribe_reason = "empty parse"
        elif is_scan(page.words, s) and s["transcribe_scans"]:
            transcribe_reason = "scan"
        elif missed_content(page, s) and s["transcribe_missed"]:
            transcribe_reason = "parser missed content"
        if transcribe_reason:
            if transcribe_reason != "scan" and uses_row_ranges(page, s):
                page.trace["route"] = f"row ranges ({transcribe_reason})"
                return [Call("rows", page, self.rows_prompt(page), page.staged)]
            page.trace["route"] = f"transcribe ({transcribe_reason})"
            return [
                Call(
                    "transcribe",
                    page,
                    TRANSCRIBE_PROMPT,
                    page.staged,
                    max_tokens=s["transcribe_max_tokens"],
                )
            ]
        page.trace["route"] = "repair"
        calls: List[Call] = []
        if s["repair_text"]:
            calls.append(
                Call(
                    "repair_text",
                    page,
                    REPAIR_TEXT_PROMPT.replace("<<TEXT>>", page.enriched or ""),
                    page.staged,
                )
            )
        if page.tables and s["repair_tables"]:
            calls.append(
                Call(
                    "repair_tables",
                    page,
                    REPAIR_TABLES_PROMPT.replace(
                        "<<TABLES>>", "\n\n".join(page.tables)
                    ),
                    page.staged,
                )
            )
        if s["charts"]:
            context = chart_context(page.content, s["chart_context_chars"])
            for image in page.images:
                calls.append(
                    Call(
                        "chart",
                        page,
                        CHART_CONTEXT_PROMPT.replace("<<CONTEXT>>", context),
                        image["path"],
                        image=image["id"],
                    )
                )
        return calls

    def rows_prompt(self, page: PageState) -> str:
        rows = str(self.settings["rows_per_call"])
        if not page.row_parts:
            return ROWS_FIRST_PROMPT.replace("<<ROWS>>", rows)
        header, last = open_table_context("\n".join(page.row_parts))
        return (
            ROWS_NEXT_PROMPT.replace("<<HEADER>>", header)
            .replace("<<LAST>>", last)
            .replace("<<ROWS>>", rows)
        )

    def finish(
        self, call: Call, text: Optional[str], error: Optional[str] = None
    ) -> List[Call]:
        """Record one answer; returns the calls it leads to (a retry, the next row
        range, or the plain chart prompt after the context prompt failed)."""
        page, s = call.page, self.settings
        page.trace.setdefault("calls", []).append(
            [call.kind, "ok" if text is not None else f"failed: {(error or '')[:200]}"]
        )
        if text is None:
            if call.attempt < s["retries"]:
                return [
                    Call(
                        call.kind,
                        page,
                        call.prompt,
                        call.file,
                        call.attempt + 1,
                        call.image,
                        call.max_tokens,
                    )
                ]
            if call.kind == "chart":
                return [
                    Call("chart_plain", page, CHART_PROMPT, call.file, image=call.image)
                ]
            return []
        if call.kind == "transcribe":
            page.transcription = text
        elif call.kind == "repair_text":
            page.repaired_text = text
        elif call.kind == "repair_tables":
            page.repaired_tables = text
        elif call.kind in ("chart", "chart_plain"):
            page.charts[call.image] = text
        elif call.kind == "rows":
            body, marker = split_marker(text)
            if not page.row_parts and "<table" not in body:
                # Not a table transcription: keep the parser's tables instead.
                page.trace.setdefault("calls", [])[-1][1] = "failed: no table in answer"
                return []
            page.row_parts.append(body)
            if marker == "MORE" and len(page.row_parts) < s["row_range_max_calls"]:
                return [Call("rows", page, self.rows_prompt(page), page.staged)]
        return []

    def run_rounds(self, calls: List[Call]) -> int:
        """Submit every pending call at once, collect them, and repeat with the calls
        their answers lead to until none is left; returns the number of calls made."""
        made = 0
        while calls:
            jobs = []
            for call in calls:
                jobs.append(
                    (
                        call,
                        self.session.sql(
                            _COMPLETE_SQL,
                            params=[
                                self.model,
                                call.prompt,
                                call.file,
                                json.dumps(
                                    {"max_tokens": call.max_tokens or self.max_tokens}
                                ),
                            ],
                        ).collect_nowait(),
                    )
                )
            made += len(jobs)
            calls = []
            for call, job in jobs:
                try:
                    text, error = read_answer(job.result()[0][0])
                except Exception as exc:
                    text, error = None, str(exc)
                calls += self.finish(call, text, error)
        return made


def run(
    session: Session,
    path: str,
    model: str,
    settings: str,
    work_stage: str,
) -> DataFrame:  # pragma: no cover - exercised against a live account
    """Parse every file under ``path`` to Markdown; one output row per file.

    Args:
        path: stage path of a file or folder, e.g. ``@stage/reports/``.
        model: the AI_COMPLETE model for the follow-up calls.
        settings: JSON of the pipeline settings (see ``_PARSE_SETTINGS`` in
            document_reader.py): max_tokens, retries, which steps run, and the row-range
            thresholds.
        work_stage: stage where page files and extracted images are written (the
            session stage).

    Returns:
        STAGE_PATH, TOTAL_PAGES, CONTENT (the pages' Markdown joined with a blank line),
        ERROR (AI_PARSE_DOCUMENT's error after one retry, or None) and TRACE (JSON: each
        page's route and the outcome of each call; not returned by the reader, for
        debugging). A failed follow-up call never
        fails the file; that part of the page keeps the parser's output.
    """
    from pypdf import PdfReader
    from snowflake.snowpark.files import SnowflakeFile
    from snowflake.snowpark.types import (
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    parse = DocumentParse(session, model, json.loads(settings), work_stage)
    # 1. Read every file and start AI_PARSE_DOCUMENT on all of them at once.
    files = []
    for row in session.sql(f"LIST {path}").collect():
        name = qualify_listed_name(path, row["name"])
        with SnowflakeFile.open(name, "rb", require_scoped_url=False) as f:
            content = f.read()
        try:
            PdfReader(io.BytesIO(content))
            is_pdf = True
        except Exception:
            is_pdf = False
        sql = _PARSE_SQL if is_pdf else _PARSE_PLAIN_SQL
        files.append(
            (name, content, is_pdf, session.sql(sql, params=[name]).collect_nowait())
        )
    # 2. Collect the parses (retrying a failed one once) and plan each PDF page's
    #    follow-up calls.

    def collect_parse(job) -> Dict[str, Any]:
        try:
            raw = job.result()[0][0]
            return json.loads(raw) if isinstance(raw, str) else raw
        except Exception as exc:
            return {"error": str(exc)}

    parsed_files = [
        (name, content, is_pdf, collect_parse(job))
        for name, content, is_pdf, job in files
    ]
    retry_jobs = {
        name: session.sql(
            _PARSE_SQL if is_pdf else _PARSE_PLAIN_SQL, params=[name]
        ).collect_nowait()
        for name, _, is_pdf, parsed in parsed_files
        if parsed.get("error")
    }
    results: Dict[str, Dict[str, Any]] = {}
    calls: List[Call] = []
    for name, content, is_pdf, parsed in parsed_files:
        parse_attempts = 1
        if name in retry_jobs:
            parsed = collect_parse(retry_jobs[name])
            parse_attempts = 2
        result = {"parsed": parsed, "pages": [], "parse_attempts": parse_attempts}
        results[name] = result
        if parsed.get("error") or not is_pdf:
            continue
        try:
            pages = parse.pages_of(name, content, parsed)
        except Exception:
            continue  # keep the plain parse
        for page in pages:
            page.text, page.tables = replace_tables(page.content)
            if parse.settings["degenerate_transcribe"] and (
                degeneracy(page.content) >= parse.settings["degenerate_threshold"]
            ):
                page.words = 0  # values look invented: transcribe the page instead
            page.enriched = parse.enrich(page, content)
            calls += parse.plan_page(page)
        result["pages"] = pages
    # 3. Run every follow-up call, in rounds, until none is left.
    parse.run_rounds(calls)
    # 4. One row per file: its pages assembled and joined in page order.
    rows = []
    for name, result in results.items():
        parsed = result["parsed"]
        error = parsed.get("error")
        value = parsed.get("value") or {}
        total = (parsed.get("metadata") or {}).get("pageCount")
        if result["pages"]:
            content = PAGE_SEPARATOR.join(
                assemble_page(p, parse.settings) for p in result["pages"]
            )
        elif value.get("pages"):
            content = PAGE_SEPARATOR.join(
                p.get("content") or "" for p in value["pages"]
            )
        else:
            content = value.get("content")
        trace = {
            "parse_attempts": result["parse_attempts"],
            "pages": [p.trace for p in result["pages"]],
        }
        rows.append(
            [
                name,
                total,
                None if error else content,
                str(error) if error else None,
                json.dumps(trace),
            ]
        )
    schema = StructType(
        [
            StructField("STAGE_PATH", StringType()),
            StructField("TOTAL_PAGES", IntegerType()),
            StructField("CONTENT", StringType()),
            StructField("ERROR", StringType()),
            StructField("TRACE", StringType()),
        ]
    )
    return session.create_dataframe(rows, schema=schema)
