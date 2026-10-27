#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import json

import pytest

from snowflake.snowpark._internal.document_parse_proc import (
    PageState,
    apply_headings,
    apply_spans,
    assemble_page,
    chart_context,
    close_tables,
    degeneracy,
    coverage,
    estimated_page_tokens,
    find_tables,
    is_scan,
    missed_content,
    open_table_context,
    read_answer,
    replace_tables,
    split_marker,
    split_row,
    splice_charts,
    splice_tables,
    strip_fences,
    table_to_html,
    uses_row_ranges,
    visible_chars,
)

SETTINGS = {
    "scan_min_words": 20,
    "tokens_per_word": 21,
    "row_range_tokens": 24000,
    "coverage_min": 0.5,
    "coverage_min_with_images": 0.15,
    "coverage_min_chars": 200,
}

# ---------------------------------------------------------------------------
# Pipe tables to HTML
# ---------------------------------------------------------------------------


class TestPipeTables:
    def test_split_row_strips_cells_and_keeps_escaped_pipes(self):
        assert split_row("| a | b \\| c |  |") == ["a", "b | c", ""]

    def test_find_tables_needs_a_separator_line(self):
        lines = ["text", "| a | b |", "|---|---|", "| 1 | 2 |", "", "| not | a table |"]
        assert find_tables(lines) == [(1, 4)]

    def test_simple_table(self):
        html = table_to_html(["| a | b |", "|---|---|", "| 1 | 2 |"])
        assert html == (
            "<table>\n<tr><th>a</th><th>b</th></tr>\n<tr><td>1</td><td>2</td></tr>\n</table>"
        )

    def test_header_label_spans_the_empty_cells_after_it(self):
        html = table_to_html(
            [
                "|  | North |  | South |",
                "|---|---|---|---|",
                "| Region | Q1 | Q2 | Q1 |",
                "| A | 1 | 2 | 3 |",
            ]
        )
        assert '<th colspan="2">North</th>' in html
        # The fully filled, non-numeric row under a header with gaps is a header row too.
        assert "<th>Region</th>" in html

    def test_sub_header_row_with_an_empty_first_cell_is_promoted(self):
        html = table_to_html(
            ["| Item | 2023 |", "|---|---|", "|  | USD |", "| A | 1 |"]
        )
        assert html.count("<tr><th") == 2

    def test_title_band_spans_the_whole_table(self):
        html = table_to_html(
            ["| Rates |  |", "|---|---|", "| Age | Rate |", "| 30 | 1.0 |"]
        )
        assert '<th colspan="2">Rates</th>' in html

    def test_side_by_side_copies_of_a_table_are_split(self):
        html = table_to_html(
            [
                "| Year | Rate | Year | Rate |",
                "|---|---|---|---|",
                "| 1 | 0.1 | 2 | 0.2 |",
            ]
        )
        assert html.count("<table>") == 2

    def test_a_body_row_repeating_the_header_starts_a_new_table(self):
        html = table_to_html(
            [
                "| Age | Rate |",
                "|---|---|",
                "| 30 | 1.0 |",
                "| Female |  |",
                "| Age | Rate |",
                "| 30 | 0.9 |",
            ]
        )
        assert html.count("<table>") == 2

    def test_cell_markup_becomes_html_and_text_is_escaped(self):
        html = table_to_html(["| a | b |", "|---|---|", "| **x** | $\\mu$g & 5<6 |"])
        assert "<strong>x</strong>" in html
        assert "µg &amp; 5&lt;6" in html

    def test_replace_tables_keeps_html_tables_in_order_with_pipe_tables(self):
        md = "a\n| x |\n|---|\n| 1 |\nb <TABLE border=1><tr><td>h</td></tr></table> c"
        text, tables = replace_tables(md)
        assert text == "a\n[[TABLE 1]]\nb \n[[TABLE 2]]\n c"
        assert tables[1] == "<TABLE border=1><tr><td>h</td></tr></table>"

    def test_replace_tables_leaves_numbered_markers(self):
        md = "Intro\n| a |\n|---|\n| 1 |\nMiddle\n| b |\n|---|\n| 2 |\nEnd"
        text, tables = replace_tables(md)
        assert text == "Intro\n[[TABLE 1]]\nMiddle\n[[TABLE 2]]\nEnd"
        assert len(tables) == 2 and all(t.startswith("<table>") for t in tables)


class TestDegeneracy:
    def test_a_grid_of_one_repeated_value_is_degenerate(self):
        rows = "\n".join(f"| r{i} | 0 | 0 | 0 | 0 | 0 |" for i in range(6))
        md = "| h | a | b | c | d | e |\n|---|---|---|---|---|---|\n" + rows
        assert degeneracy(md) == 1.0

    def test_small_tables_are_ignored(self):
        assert degeneracy("| h | a |\n|---|---|\n| r | 0 |\n| s | 0 |") == 0.0

    def test_a_varied_table_is_not_degenerate(self):
        rows = "\n".join(
            f"| r{i} | {i} | {i * 2} | {i * 3} | {i * 4} | {i * 5} |"
            for i in range(1, 7)
        )
        md = "| h | a | b | c | d | e |\n|---|---|---|---|---|---|\n" + rows
        assert degeneracy(md) < 0.5


# ---------------------------------------------------------------------------
# Styling from the text layer
# ---------------------------------------------------------------------------


class TestStyling:
    def test_a_unique_span_is_wrapped(self):
        assert apply_spans("The total amount due", [("b", "amount")]) == (
            "The total **amount** due"
        )

    def test_an_ambiguous_span_is_skipped(self):
        md = "amount and amount"
        assert apply_spans(md, [("b", "amount")]) == md

    def test_a_short_span_is_skipped_but_a_superscript_is_not(self):
        assert apply_spans("ab cd", [("b", "ab")]) == "ab cd"
        assert apply_spans("Note1 here", [("sup", "1")]) == "Note<sup>1</sup> here"

    def test_a_span_matches_across_line_breaks(self):
        assert apply_spans("very\nimportant", [("i", "very important")]) == (
            "*very\nimportant*"
        )

    def test_a_whole_heading_line_is_not_wrapped(self):
        md = "# Summary\ntext"
        assert apply_spans(md, [("b", "Summary")]) == md

    def test_headings_are_rewritten_at_their_level(self):
        md = "Annual Report\nbody text\n**Results**"
        out = apply_headings(md, {"annual report": 1, "results": 2})
        assert out == "# Annual Report\nbody text\n## Results"


# ---------------------------------------------------------------------------
# Answers
# ---------------------------------------------------------------------------


class TestAnswers:
    def test_read_answer_returns_the_text(self):
        assert read_answer(json.dumps({"value": " hi ", "error": None})) == (
            "hi",
            None,
        )

    def test_read_answer_treats_an_empty_answer_as_an_error(self):
        assert read_answer({"value": "  ", "error": None}) == (None, "empty answer")
        assert read_answer({"value": None, "error": "boom"}) == (None, "boom")

    def test_strip_fences(self):
        assert strip_fences("```html\n<table></table>\n```") == "<table></table>"
        assert strip_fences("plain") == "plain"

    def test_chart_context_blanks_placeholders_and_truncates(self):
        assert chart_context("a ![img-0.jpeg](img-0.jpeg) b", 9) == "a [image]"

    def test_split_marker(self):
        assert split_marker("<tr></tr>\nMORE") == ("<tr></tr>", "MORE")
        assert split_marker("<tr></tr>\nDONE\n") == ("<tr></tr>", "DONE")
        assert split_marker("<tr></tr>") == ("<tr></tr>", "")

    def test_open_table_context(self):
        text = "<table><tr><th>A</th></tr><tr><td>1</td></tr><tr><td>2</td></tr>"
        assert open_table_context(text) == (
            "<tr><th>A</th></tr>",
            "<tr><td>2</td></tr>",
        )
        assert open_table_context("") == ("(none)", "(none)")

    def test_close_tables(self):
        assert close_tables("<table><tr></tr>") == "<table><tr></tr>\n</table>"
        assert close_tables("<table></table>") == "<table></table>"


# ---------------------------------------------------------------------------
# Routing and assembly
# ---------------------------------------------------------------------------


def _page(**kwargs) -> PageState:
    base = dict(file="@s/a.pdf", index=0, content="", images=[])
    base.update(kwargs)
    return PageState(**base)


class TestRouting:
    @pytest.mark.parametrize("words, scan", [(0, True), (19, True), (20, False)])
    def test_is_scan(self, words, scan):
        assert is_scan(words, SETTINGS) is scan

    def test_row_ranges_only_for_large_pages(self):
        assert estimated_page_tokens(_page(words=1000), SETTINGS) == 21000
        assert not uses_row_ranges(_page(words=1000), SETTINGS)
        assert uses_row_ranges(_page(words=1200), SETTINGS)

    def test_visible_chars_ignores_whitespace_markup_and_images(self):
        assert visible_chars("| a | **b** |\n|---|---|\n# c ![x](img-0.jpeg)") == 3

    def test_coverage_and_missed_content(self):
        page = _page(content="abc " * 40, layer_chars=400)
        assert coverage(page) == pytest.approx(0.3)
        assert missed_content(page, SETTINGS)
        assert not missed_content(_page(content="abc " * 40, layer_chars=150), SETTINGS)
        assert not missed_content(_page(content="abc " * 80, layer_chars=400), SETTINGS)
        assert coverage(_page(content="x", layer_chars=0)) == 1.0
        with_image = _page(content="abc " * 40, layer_chars=400, images=[{"id": "i"}])
        assert not missed_content(with_image, SETTINGS)
        nearly_empty = _page(content="abc", layer_chars=400, images=[{"id": "i"}])
        assert missed_content(nearly_empty, SETTINGS)


class TestSplicing:
    def test_tables_replace_their_markers(self):
        out = splice_tables("a\n[[TABLE 1]]\nb\n[[TABLE 2]]", ["<t1>", "<t2>"])
        assert out == "a\n<t1>\nb\n<t2>"

    def test_a_table_whose_marker_was_dropped_is_appended(self):
        assert splice_tables("a", ["<t1>", "<t2>"]) == "a\n\n<t1>\n\n<t2>"

    def test_one_repaired_block_goes_at_the_first_marker(self):
        out = splice_tables("a\n[[TABLE 1]]\nb\n[[TABLE 2]]\nc", ["<both>"])
        assert out == "a\n<both>\nb\n\nc"

    def test_one_block_with_no_marker_is_appended(self):
        assert splice_tables("a", ["<t>"]) == "a\n\n<t>"

    def test_charts_replace_placeholders_and_missing_ones_are_appended(self):
        text = "x ![img-0.jpeg](img-0.jpeg) y"
        out = splice_charts(text, {"img-0.jpeg": "<c0>", "img-1.jpeg": "<c1>"})
        assert out == "x \n\n<c0>\n\n y\n\n<c1>"

    def test_a_placeholder_without_an_answer_is_kept(self):
        assert splice_charts("![a](img-0.jpeg)", {}) == "![a](img-0.jpeg)"


class TestAssemblePage:
    def test_a_transcription_wins(self):
        page = _page(content="parsed", transcription=" whole page ")
        assert assemble_page(page, SETTINGS) == "whole page"

    def test_repaired_text_with_repaired_tables_and_charts(self):
        page = _page(
            content="raw",
            text="T\n[[TABLE 1]]\n![img-0.jpeg](img-0.jpeg)",
            tables=["<parser table>"],
            enriched="E\n[[TABLE 1]]\n![img-0.jpeg](img-0.jpeg)",
            repaired_text="R\n[[TABLE 1]]\n![img-0.jpeg](img-0.jpeg)",
            repaired_tables="<repaired>",
            charts={"img-0.jpeg": "<chart>"},
        )
        assert assemble_page(page, SETTINGS) == "R\n<repaired>\n\n\n<chart>"

    def test_without_repairs_the_enriched_text_and_parser_tables_are_used(self):
        page = _page(
            content="raw",
            text="T\n[[TABLE 1]]",
            tables=["<parser table>"],
            enriched="**T**\n[[TABLE 1]]",
        )
        assert assemble_page(page, SETTINGS) == "**T**\n<parser table>"

    def test_row_ranges_are_joined_closed_and_keep_the_parsers_text(self):
        page = _page(
            content="raw",
            text="Title\n[[TABLE 1]]",
            tables=["<parser table>"],
            row_parts=["<table><tr><td>1</td></tr>", "<tr><td>2</td></tr>"],
        )
        assert assemble_page(page, SETTINGS) == (
            "Title\n<table><tr><td>1</td></tr>\n<tr><td>2</td></tr>\n</table>"
        )
