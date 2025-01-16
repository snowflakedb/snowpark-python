#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401


@pytest.fixture(scope="function")
def a():
    return np.array(
        [
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
        dtype=object,
    )


@pytest.fixture(scope="function")
def b():
    return np.array(
        [
            "one",
            "one",
            "one",
            "two",
            "one",
            "one",
            "one",
            "two",
            "two",
            "two",
            "one",
        ],
        dtype=object,
    )


@pytest.fixture(scope="function")
def c():
    return np.array(
        [
            "dull",
            "dull",
            "shiny",
            "dull",
            "dull",
            "shiny",
            "shiny",
            "dull",
            "shiny",
            "shiny",
            "shiny",
        ],
        dtype=object,
    )


@pytest.fixture(scope="function")
def basic_crosstab_dfs():
    df = native_pd.DataFrame(
        {
            "species": ["dog", "cat", "dog", "dog", "cat", "cat", "dog", "cat"],
            "favorite_food": [
                "chicken",
                "fish",
                "fish",
                "beef",
                "chicken",
                "beef",
                "fish",
                "beef",
            ],
            "age": [7, 2, 8, 5, 9, 3, 6, 1],
        }
    )
    return df, pd.DataFrame(df)
