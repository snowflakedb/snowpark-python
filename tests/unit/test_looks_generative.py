#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
"""Regression tests for the looks_generative() fix.

Every FALSE-POSITIVE case here reproduces a real, measured trigger from the
57-family extractbench corpus (family / field / match-type / location
recorded in /tmp/lg/rows.json under the pre-fix predicate) as a minimal
schema, so each test isolates exactly the mechanism that was fixed:

  - dropping the _COMPLETE_PHRASES scan of field DESCRIPTIONS (the source of
    27 of the 29 pre-fix false positives), and
  - requiring an exact underscore-token match on the field NAME plus a
    RESOLVED-type gate that skips object/array fields (the source of the
    other 2).

Two of these schemas embed the field verbatim as it exists in the real
representative case (990PF's transaction-table negation, cbp_7501's
cross-reference to a sibling field, professional_valuation's $ref'd
Valuation Summary object) rather than paraphrasing, per the report
requirement to assert the actual triggering strings.

NOTE on cbp_7501: the pre-fix false positive on entry_type ("... or
summary_status") is what test_cbp_7501_cross_reference_no_longer_matches
isolates and confirms is fixed. The cbp_7501 FAMILY's overall verdict is
unchanged (still True) because a different field in that same schema,
summary_date ("Block 3 Summary Date ... transcribe ... do NOT normalize"),
now trips the exact-token name match on "summary" -- it is a printed date
field whose form label happens to be "Summary Date", not a generative
field. That is a known residual gap in this fix, not something this test
suite hides: see test_cbp_7501_summary_date_is_a_residual_false_positive
below, and the concerns section of the accompanying report.
"""
from snowflake.snowpark._internal.document_reader_heuristics import looks_generative

# --- Real triggering field from 990PF:593338187 (rep_id
# short/593338187_200912_990PF-p0023) and roger_winfield_990pf, field
# "transactions". Pre-fix this matched phrase:summar in the DESCRIPTION --
# the description's own instruction is a NEGATION telling the model to
# EXCLUDE summary lines from this field, the single most damning example
# of why description-prose scanning cannot stand in for judging what the
# field itself asks for.
NINETY_PF_SCHEMA = {
    "type": "object",
    "properties": {
        "transactions": {
            "anyOf": [
                {"type": "array", "items": {"$ref": "#/$defs/TransactionRow"}},
                {"type": "null"},
            ],
            "default": None,
            "title": "Transactions",
            "description": (
                "Every individual sale line in the 1099-B proceeds table on "
                "this page, in printed top-to-bottom order. Each row carries "
                "a CUSIP '(Box 1b)' and share count '(Box 5)'. Do NOT "
                "include section-summary lines such as 'Total Short Term "
                "Sales Reported on 1099-B' here — those go in "
                "section_subtotals. Null only if the page prints no "
                "transaction rows."
            ),
        }
    },
    "$defs": {
        "TransactionRow": {
            "type": "object",
            "title": "1099-B transaction",
            "properties": {
                "cusip": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                "number_of_shares": {"anyOf": [{"type": "number"}, {"type": "null"}]},
            },
        }
    },
}

# --- Real triggering field from fidelity_0002_sample (rep_id
# short/fidelity_0002_sample), field "tax_year". Pre-fix this matched
# phrase:explain in the DESCRIPTION -- the field itself is a 4-digit
# integer lifted from a page header; "explainer-only PDFs" describes a
# CLASS OF SOURCE DOCUMENT the field should return null for, not anything
# the model composes.
FIDELITY_SCHEMA = {
    "type": "object",
    "properties": {
        "tax_year": {
            "type": ["integer", "null"],
            "description": (
                "4-digit tax year for this statement, from the page header "
                "'TAX YEAR YYYY' / '2025 TAX AND YEAR-END STATEMENT' / "
                "'2024 Consolidated 1099 Tax Statement'. Return null only if "
                "no tax year is printed (extremely rare on actual "
                "statements; common on explainer-only PDFs which should "
                "return null for all top-level fields)."
            ),
        }
    },
}

# --- Real triggering field from real_wyo (rep_id
# short/real_wyo_Goshen_2024), field "election_title". Pre-fix this matched
# phrase:summar in the DESCRIPTION -- "Summary" only appears inside the
# quoted EXAMPLE VALUE the description gives ('Official
# Precinct-by-Precinct Summary'), not as an instruction to compose one.
REAL_WYO_SCHEMA = {
    "type": "object",
    "properties": {
        "election_title": {
            "type": "string",
            "description": (
                "Report title from the header, e.g. 'Official "
                "Precinct-by-Precinct Summary'."
            ),
        }
    },
}

# --- Real triggering field from cbp_7501 (rep_id
# short/cbp_7501_continuation_0001), field "entry_type". Pre-fix this
# matched phrase:summar in the DESCRIPTION -- the only occurrence of
# "summary" is a cross-reference telling the model not to confuse this
# field with the SIBLING field summary_status; entry_type itself is a
# verbatim 2-digit code transcription.
CBP_ENTRY_TYPE_SCHEMA = {
    "type": "object",
    "properties": {
        "entry_type": {
            "anyOf": [{"type": "string"}, {"type": "null"}],
            "default": None,
            "title": "Entry Type",
            "description": (
                "Block 2 Entry Type code as printed (typically 2-digit "
                "code, sometimes with a single-letter suffix, e.g. '01', "
                "'02 ABI/A', '01 ABI/A'). Verbatim — preserve the "
                "trailing ' ABI/A' or other suffixes if printed (the "
                "suffix indicates Automated Broker Interface filing "
                "track). Do NOT confuse with bond_type (block 5, a single "
                "digit), entry_number, or summary_status."
            ),
        }
    },
}

# --- The field on the SAME cbp_7501 representative case that keeps the
# family's overall verdict True after the fix. Real field, real
# description; included so the residual gap is asserted, not just
# mentioned in prose. summary_date is a printed date field ("Block 3
# Summary Date") the model transcribes verbatim -- not generative -- but
# its name legitimately contains the exact token "summary", which is
# exactly the class of case an exact-token name match cannot distinguish
# from a real composition request without also reading the description
# (which this fix deliberately stops doing, for the reasons documented on
# looks_generative itself).
CBP_SUMMARY_DATE_SCHEMA = {
    "type": "object",
    "properties": {
        "summary_date": {
            "type": "string",
            "description": (
                "Block 3 Summary Date as printed in the top row. Format "
                "MM/DD/YYYY (4-digit year, e.g. '12/16/2020', '10/23/2020 "
                "BBA', '05/19/22'). Preserve any trailing tracking "
                "annotations printed in the same cell (e.g. 'BBA' on PDF "
                "0002) by including them verbatim with the date. For "
                "2-digit years as printed (e.g. '05/19/22'), keep the "
                "printed 2-digit form — do NOT normalize. Do NOT "
                "confuse with entry_date (block 7), import_date (block "
                "11), export_date (block 15), or it_date (block 17)."
            ),
        }
    },
}

# --- Real triggering field from professional_valuation (rep_id
# short/3N1AB7AP8FY283932_professional_valuation), field
# "valuation_summary". Pre-fix this matched exact-token:summary on the
# NAME (not a phrase/description match), and it is genuinely a false
# positive: the field is a $ref'd OBJECT whose own properties
# (base_value, condition_adjustment, ...) are each independently extracted
# numbers, i.e. an aggregation container, not a composed narrative --
# composed narrative can only be a scalar leaf.
PROFESSIONAL_VALUATION_SCHEMA = {
    "type": "object",
    "properties": {
        "valuation_summary": {
            "$ref": "#/$defs/ValuationSummary",
            "description": (
                "Loss-vehicle Valuation Summary block from page 1 — "
                "includes Base Value, the four Loss Vehicle Adjustments "
                "(Condition, Prior Damage, Aftermarket Parts, "
                "Refurbishment), Market Value, Settlement Adjustments "
                "(Taxes, Fees, Deductible, Post-Tax adjustment), and the "
                "final Settlement Value."
            ),
        }
    },
    "$defs": {
        "ValuationSummary": {
            "type": "object",
            "title": "ValuationSummary",
            "properties": {
                "base_value": {"anyOf": [{"type": "number"}, {"type": "null"}]},
                "condition_adjustment": {
                    "anyOf": [{"type": "number"}, {"type": "null"}]
                },
            },
        }
    },
}

# --- Real triggering field from cfpb_closing (rep_id
# short/cfpb_closing-disclosure_H25E), field
# "estimated_taxes_insurance_assessments". Pre-fix this matched
# substring-token:assessment on the NAME -- "assessment" (singular, in
# _COMPLETE_TOKENS) is a bare substring of "assessments" (plural, the
# field's actual underscore-split token), which the exact-token-only match
# no longer allows.
CFPB_SCHEMA = {
    "type": "object",
    "properties": {
        "estimated_taxes_insurance_assessments": {
            "anyOf": [{"type": "string"}, {"type": "null"}],
            "default": None,
            "title": "Estimated Taxes, Insurance & Assessments",
            "description": (
                "The 'Estimated Taxes, Insurance & Assessments' monthly "
                "amount from the Projected Payments section (page 1), the "
                "'$x a month' figure, verbatim dollar only e.g. '$356.13' "
                "(omit the trailing 'a month'). Do NOT confuse with "
                "estimated_total_monthly_payment or the Estimated Escrow "
                "line. Null if absent."
            ),
        }
    },
}

# --- Constructed (the corpus contains no real example of a genuinely
# generative field -- see the Part 2 false-negative audit): the predicate
# must not be disabled outright by this fix. A scalar string field whose
# name carries the exact token "summary" AND whose description asks the
# model to COMPOSE a narrative synthesis, not transcribe or classify
# anything printed, must still return True.
GENUINELY_GENERATIVE_SCHEMA = {
    "type": "object",
    "properties": {
        "case_summary": {
            "type": "string",
            "description": (
                "Write a 2-3 sentence narrative summary of this case in "
                "your own words, synthesizing the key facts above. Do not "
                "copy sentences verbatim from the source document."
            ),
        }
    },
}

# --- Constructed: exercises the array side of the resolved-type gate,
# which a name match on a table/row container ("recommendation_list") must
# not pass, and which the corpus's real object-typed example
# (valuation_summary, above) does not by itself demonstrate.
GENERATIVE_NAME_BUT_ARRAY_SCHEMA = {
    "type": "object",
    "properties": {
        "recommendation_list": {
            "type": "array",
            "items": {"type": "string"},
            "description": (
                "List of discrete recommendation strings printed in the "
                "report's numbered recommendations section, one per list "
                "item, verbatim as printed."
            ),
        }
    },
}


class TestLooksGenerativeDropsDescriptionScanning:
    def test_990pf_negation_about_a_different_field_is_not_generative(self):
        # "Do NOT include summary lines" is an instruction about what to
        # EXCLUDE from this field, not a request to compose one.
        assert looks_generative(NINETY_PF_SCHEMA) is False

    def test_fidelity_explainer_only_pdfs_is_not_generative(self):
        # "explainer-only PDFs" describes a class of source document this
        # integer field should return null for.
        assert looks_generative(FIDELITY_SCHEMA) is False

    def test_real_wyo_example_string_is_not_generative(self):
        # "Summary" appears only inside a quoted example VALUE, not as an
        # instruction.
        assert looks_generative(REAL_WYO_SCHEMA) is False

    def test_cbp_7501_cross_reference_no_longer_matches(self):
        # entry_type's only mention of "summary" is a cross-reference to
        # the sibling field summary_status; entry_type itself is a
        # verbatim code transcription.
        assert looks_generative(CBP_ENTRY_TYPE_SCHEMA) is False


class TestLooksGenerativeExactTokenNameMatch:
    def test_cfpb_assessments_plural_no_longer_matches(self):
        # "assessments" (plural) is the field's actual token; "assessment"
        # (singular) is only a _COMPLETE_TOKENS member, and the two are no
        # longer treated as equal under exact-token matching.
        assert looks_generative(CFPB_SCHEMA) is False

    def test_professional_valuation_summary_object_is_gated_out(self):
        # valuation_summary's name carries the exact token "summary", but
        # it resolves (via the $ref) to an object whose properties are all
        # independently-extracted numbers -- an aggregation container, not
        # a scalar the model could compose narrative into.
        assert looks_generative(PROFESSIONAL_VALUATION_SCHEMA) is False

    def test_generative_name_on_array_field_is_gated_out(self):
        # Same reasoning as the object case: an array of printed strings
        # is a transcription target, not something a scalar leaf could
        # compose.
        assert looks_generative(GENERATIVE_NAME_BUT_ARRAY_SCHEMA) is False


class TestLooksGenerativeStillFiresOnRealComposition:
    def test_constructed_narrative_field_still_returns_true(self):
        # The fix must not disable the predicate outright: a scalar string
        # field whose name matches _COMPLETE_TOKENS and whose description
        # asks for composed narrative must still return True.
        assert looks_generative(GENUINELY_GENERATIVE_SCHEMA) is True


class TestLooksGenerativeKnownResidualGap:
    def test_cbp_7501_summary_date_is_a_residual_false_positive(self):
        # Documented, not silently accepted: summary_date is a printed
        # date field ("Block 3 Summary Date ... do NOT normalize") whose
        # form label happens to contain the exact token "summary". This
        # fix does not and cannot distinguish this from a real composition
        # request without reading the description again -- which is
        # exactly the scan this fix removes, for good reason (see
        # looks_generative's docstring). This is why cbp_7501's overall
        # family verdict is unchanged after the fix even though the
        # specific mechanism that used to trip it (entry_type's
        # cross-reference, tested above) is fixed.
        assert looks_generative(CBP_SUMMARY_DATE_SCHEMA) is True
