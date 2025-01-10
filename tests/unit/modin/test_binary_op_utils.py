#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark.modin.plugin._internal.binary_op_utils import (
    merge_label_and_identifier_pairs,
)


@pytest.mark.parametrize(
    "sorted_labels,a,b,expected",
    [
        ([1, 2, 3], [(1, "A"), (3, "C")], [(2, "B")], [(1, "A"), (2, "B"), (3, "C")]),
        ([1, 2, 3], [(1, "A"), (2, "B"), (3, "C")], [], [(1, "A"), (2, "B"), (3, "C")]),
        ([1, 2, 3], [], [(1, "A"), (2, "B"), (3, "C")], [(1, "A"), (2, "B"), (3, "C")]),
    ],
)
def test_merge_sorted_labels(sorted_labels, a, b, expected):
    assert merge_label_and_identifier_pairs(sorted_labels, a, b) == expected
