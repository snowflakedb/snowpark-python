#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
"""Assertions for session.generator + SEQ* samples.

Snowflake SEQ* values are not unique under parallel generation. After ORDER BY
they are non-decreasing (ties allowed). CI failed when #4306 required ``<``.
"""

from typing import Callable, Sequence


def seqs_are_nondecreasing(seqs: Sequence) -> bool:
    return all(a <= b for a, b in zip(seqs, seqs[1:]))


def seqs_are_strictly_increasing(seqs: Sequence) -> bool:
    return all(a < b for a, b in zip(seqs, seqs[1:]))


def assert_ordered_uniform_sample(
    rows: Sequence,
    *,
    seq_getter: Callable = lambda row: row[0],
    uniform_getter: Callable = lambda row: row[1],
    expected_len: int = 3,
    expected_uniform: int = 3,
) -> None:
    """Check a LIMIT 3 generator sample: 3 rows, deterministic uniform, ordered SEQ."""
    assert len(rows) == expected_len
    seqs = [seq_getter(row) for row in rows]
    uniforms = [uniform_getter(row) for row in rows]
    assert uniforms == [expected_uniform] * expected_len
    assert seqs_are_nondecreasing(
        seqs
    ), f"SEQ values not non-decreasing after ORDER BY: {seqs}"
