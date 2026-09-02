#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
"""Unit coverage for the merge-gate SEQ ORDER BY contract.

Does not need Snowflake. Encodes the exact CI failure from
test_generator_table_function (assert 0 < 0) and the historical
#4306 expected rows.
"""

import pytest

from tests.generator_seq_asserts import (
    assert_ordered_uniform_sample,
    seqs_are_nondecreasing,
    seqs_are_strictly_increasing,
)

# (name, seqs, uniforms) — all are valid ORDER BY outputs except decreasing.
CI_AND_HISTORICAL_SAMPLES = [
    ("ci_assert_0_lt_0", [0, 0, 0], [3, 3, 3]),
    ("historical_seq1_offset20", [-108, -107, 0], [3, 3, 3]),
    ("strictly_increasing", [0, 1, 2], [3, 3, 3]),
    ("tied_nonzero", [5, 5, 5], [3, 3, 3]),
    ("partial_tie", [0, 0, 1], [3, 3, 3]),
]


@pytest.mark.parametrize("name,seqs,uniforms", CI_AND_HISTORICAL_SAMPLES)
def test_order_by_samples_are_nondecreasing(name, seqs, uniforms):
    assert seqs_are_nondecreasing(seqs), name
    rows = list(zip(seqs, uniforms))
    assert_ordered_uniform_sample(rows)


@pytest.mark.parametrize("name,seqs,uniforms", CI_AND_HISTORICAL_SAMPLES)
def test_strict_less_than_rejects_ties_that_broke_ci(name, seqs, uniforms):
    """#4306 used ``<``. That is what failed merge-gate with assert 0 < 0."""
    if seqs_are_strictly_increasing(seqs):
        assert seqs[0] < seqs[1] < seqs[2]
    else:
        with pytest.raises(AssertionError):
            assert seqs[0] < seqs[1] < seqs[2]


def test_ci_failure_values_exactly():
    rows = [(0, 3), (0, 3), (0, 3)]
    assert not seqs_are_strictly_increasing([0, 0, 0])
    assert_ordered_uniform_sample(rows)


def test_historical_seq1_offset_values_exactly():
    rows = [(-108, 3), (-107, 3), (0, 3)]
    assert seqs_are_strictly_increasing([-108, -107, 0])
    assert_ordered_uniform_sample(rows)


def test_decreasing_seq_is_not_a_valid_order_by_result():
    assert not seqs_are_nondecreasing([2, 1, 0])
    with pytest.raises(AssertionError, match="not non-decreasing"):
        assert_ordered_uniform_sample([(2, 3), (1, 3), (0, 3)])


def test_wrong_uniform_is_rejected():
    with pytest.raises(AssertionError):
        assert_ordered_uniform_sample([(0, 3), (0, 4), (0, 3)])


def test_wrong_row_count_is_rejected():
    with pytest.raises(AssertionError):
        assert_ordered_uniform_sample([(0, 3), (0, 3)])


def test_aliased_pixel_unicorn_accessors():
    rows = [
        {"PIXEL": 0, "UNICORN": 3},
        {"PIXEL": 0, "UNICORN": 3},
        {"PIXEL": 0, "UNICORN": 3},
    ]
    assert_ordered_uniform_sample(
        rows,
        seq_getter=lambda row: row["PIXEL"],
        uniform_getter=lambda row: row["UNICORN"],
    )
