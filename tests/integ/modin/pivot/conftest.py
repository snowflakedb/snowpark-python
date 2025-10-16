#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import numpy as np
import pytest


@pytest.fixture
def df_data():
    return {
        "A": [
            "foo",
            "foo",
            "foo",
            "foo",
            "bar",
            "bar",
            "bar",
            "bar",
            "foo",
            "foo",
            "foo",
        ],
        "B": [
            "on.e",
            "on.e",
            "on.e",
            'tw"o',
            "on.e",
            "on.e",
            "on.e",
            'tw"o',
            'tw"o',
            'tw"o',
            "on.e",
        ],
        "C": [
            "dull",
            "dull",
            "shi'ny",
            "dull",
            "dull",
            "shi'ny",
            "shi'ny",
            "dull",
            "shi'ny",
            "shi'ny",
            "shi'ny",
        ],
        "D": np.arange(0, 11),
        "E": np.arange(1, 12),
        "F": np.arange(2, 13),
    }


@pytest.fixture
def df_data_more_pivot_values():
    return {
        "A": [
            "foo",
            "foo",
            "foo",
            "foo",
            "bar",
            "bar",
            "bar",
            "bar",
            "foo",
            "foo",
            "foo",
            "bar",
            "bar",
            "foo",
            "foo",
            "foo",
        ],
        "B": [
            "on.e",
            "on.e",
            "on.e",
            'tw"o',
            "on.e",
            "on.e",
            "on.e",
            'tw"o',
            'tw"o',
            'tw"o',
            "on.e",
            "thr.ee",
            "thr.ee",
            "thr.ee",
            "on.e",
            'tw"o',
        ],
        "C": [
            "dull",
            "dull",
            "shi'ny",
            "dull",
            "dull",
            "shi'ny",
            "shi'ny",
            "dull",
            "shi'ny",
            "shi'ny",
            "shi'ny",
            "dull",
            "shi'ny",
            "pla.in",
            "pla.in",
            "pla.in",
        ],
        "D": np.arange(0, 16),
        "E": np.arange(1, 17),
        "F": np.arange(2, 18),
    }


@pytest.fixture
def df_data_with_duplicates():
    return (
        # NOTE: This call to np.array converts all the numbers to string
        np.array(
            [
                [
                    "foo",
                    "one",
                    "dull",
                    0,
                    1,
                    2,
                    3,
                ],
                [
                    "foo",
                    "one",
                    "dull",
                    1,
                    2,
                    3,
                    4,
                ],
                [
                    "foo",
                    "one",
                    "shiny",
                    2,
                    3,
                    4,
                    5,
                ],
                [
                    "foo",
                    "two",
                    "dull",
                    3,
                    4,
                    5,
                    6,
                ],
                [
                    "bar",
                    "one",
                    "dull",
                    4,
                    5,
                    6,
                    7,
                ],
                [
                    "bar",
                    "one",
                    "shiny",
                    5,
                    6,
                    7,
                    8,
                ],
                [
                    "bar",
                    "two",
                    "shiny",
                    6,
                    7,
                    8,
                    9,
                ],
            ]
        ),
        ["A", "A", "C", "D", "D", "E", "F"],
    )


@pytest.fixture
def df_encoded_data():
    return {
        "A": [
            'fo"o',
            "foo",
            "foo",
            'fo"o',
            "bar",
            "bar",
            "bar",
            "bar",
            "foo",
            "foo",
            "foo",
            "foo",
        ],
        "B": [
            "b\\ar",
            '"bar',
            'b"ar"',
            'bar","',
            None,
            'bar"',
            ",",
            '","',
            "b\\ar",
            "b\nar",
            "b\tar",
            "','",
        ],
        "C": [
            "dull",
            'du"ll',
            "sh\niny",
            "dull",
            "dull",
            "shiny",
            "shiny",
            "dull",
            "shi\\ny",
            "shiny",
            "sh\niny",
            None,
        ],
        "D": np.arange(0, 12),
    }


@pytest.fixture
def df_small_encoded_data():
    return {
        "A": [
            "foo",
        ],
        "B": [
            "one",
        ],
        "C": [
            "shi\\'ny",
        ],
        "D": [
            123,
        ],
    }


@pytest.fixture
def df_data_small():
    return {
        "ax": [
            "foo",
            "foo",
            "foo",
            "bar",
            "bar",
            "bar",
        ],
        "aX": [
            "one",
            "two",
            "two",
            "one",
            "one",
            "two",
        ],
        "AX": [
            "dull",
            "dull",
            "shiny",
            "dull",
            "shiny",
            "dull",
        ],
        "DS": np.arange(0, 6),
        "ET": [0, 1, None, None, None, 2],
        "FV": [None, None, None, None, None, None],
    }


@pytest.fixture
def df_data_with_nulls():
    return {
        "A": [
            "foo",
            "foo",
            "foo",
            "foo",
            "bar",
            "bar",
            "bar",
            "bar",
            "foo",
            "foo",
            "foo",
            "foo",
            "foo",
            "bar",
        ],
        "B": [
            "baz",
            "baz",
            "baz",
            "baz",
            "baz",
            "baz",
            "baz",
            "baz",
            "baz",
            "baz",
            "buz",
            "buz",
            "baz",
            "baz",
        ],
        "C": [
            "dull",
            "dull",
            "shiny",
            "dull",
            "dull",
            "shiny",
            "shiny",
            "dull",
            "dull",
            "shiny",
            "shiny",
            "shiny",
            "spot",
            "spot",
        ],
        "D": np.arange(0, 14),
        "E": [0, 1, 2, 3, None, 5, 6, 7, 8, None, 10, None, None, None],
        "F": [
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ],
    }


@pytest.fixture
def df_data_with_nulls_2():
    return {
        "A": ["foo", "bar"],
        "B": ["shiny", "dull"],
        "C": ["up", "down"],
        "D": [None, None],
        "E": [1, 2],
        "F": [None, 2],
    }


@pytest.fixture
def df_pivot_data():
    return {
        "foo": ["one", "one", "one", "two", "two", "two"],
        "bar": ["A", "B", "C", "A", "B", "C"],
        "baz": [1, 2, 3, 4, 5, 6],
        "zoo": ["x", "y", "z", "q", "w", "t"],
    }
