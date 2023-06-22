#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark import Column


@pytest.mark.localtest
def test_sort_different_inputs(session):
    df = session.create_dataframe(
        [(1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (2, 3), (3, 1), (3, 2), (3, 3)]
    ).to_df(["a", "b"])

    # str and asc
    sorted_rows = df.sort("a", ascending=True).collect()
    for idx in range(1, len(sorted_rows)):
        assert sorted_rows[idx - 1][0] <= sorted_rows[idx][0]

    # Column and desc
    sorted_rows = df.sort(Column("a"), ascending=False).collect()
    for idx in range(1, len(sorted_rows)):
        assert sorted_rows[idx - 1][0] >= sorted_rows[idx][0]

    # str, str and order default (asc)
    sorted_rows = df.sort("a", "b").collect()
    for idx in range(1, len(sorted_rows)):
        assert sorted_rows[idx - 1][0] < sorted_rows[idx][0] or (
            sorted_rows[idx - 1][0] == sorted_rows[idx][0]
            and sorted_rows[idx - 1][1] <= sorted_rows[idx][1]
        )

    # List[str] and order list
    sorted_rows = df.sort(["a", "b"], ascending=[True, False]).collect()
    for idx in range(1, len(sorted_rows)):
        assert sorted_rows[idx - 1][0] < sorted_rows[idx][0] or (
            sorted_rows[idx - 1][0] == sorted_rows[idx][0]
            and sorted_rows[idx - 1][1] >= sorted_rows[idx][1]
        )

    # List[Union[str, Column]] and order list overwrites the column
    sorted_rows = df.sort(["a", Column("b").desc()], ascending=[1, 1]).collect()
    for idx in range(1, len(sorted_rows)):
        assert sorted_rows[idx - 1][0] < sorted_rows[idx][0] or (
            sorted_rows[idx - 1][0] == sorted_rows[idx][0]
            and sorted_rows[idx - 1][1] <= sorted_rows[idx][1]
        )


@pytest.mark.localtest
def test_sort_invalid_inputs(session):
    df = session.create_dataframe(
        [(1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (2, 3), (3, 1), (3, 2), (3, 3)]
    ).to_df(["a", "b"])
    # empty
    with pytest.raises(ValueError) as ex_info:
        df.sort()
    assert "sort() needs at least one sort expression" in str(ex_info)
    with pytest.raises(ValueError) as ex_info:
        df.sort([])
    assert "sort() needs at least one sort expression" in str(ex_info)

    # invalid ascending type
    with pytest.raises(TypeError) as ex_info:
        df.sort("a", ascending="ASC")
    assert "ascending can only be boolean or list" in str(ex_info)

    # inconsistent ascending length
    with pytest.raises(ValueError) as ex_info:
        df.sort("a", "b", ascending=[True, True, True])
    assert (
        "The length of col (2) should be same with the length of ascending (3)"
        in str(ex_info)
    )

    # invalid input types
    with pytest.raises(TypeError) as ex_info:
        df.sort(["a"], ["b"])
    assert (
        "sort() only accepts str and Column objects,"
        " or a list containing str and Column objects" in str(ex_info)
    )
