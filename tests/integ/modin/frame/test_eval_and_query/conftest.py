#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest
import pandas as native_pd
from tests.integ.modin.utils import (
    create_test_dfs,
)


@pytest.fixture
def test_dfs():
    return create_test_dfs(
        native_pd.DataFrame(
            {
                "CUSTOMER_KEY": [-10, -10, -5, 30, 0, 1, 25],
                "ACCOUNT_BALANCE": [-101, -101, -51, 30, 0, 53, 105],
                "MARKET_SEGMENT": [
                    "AUTOMOBILE",
                    "AUTOMOBILE",
                    "FURNITURE",
                    "AUTOMOBILE",
                    "FURNITURE",
                    "MACHINERY",
                    "HOUSEHOLD",
                ],
                "PURCHASE COUNT": [1, 0, 5, 7, 9, 10, 0],
            },
            index=["i0", "i1", "i2", "i3", "i4", "i5", "i6"],
        )
    )


@pytest.fixture
def test_dfs_with_named_index():
    return create_test_dfs(
        native_pd.DataFrame(
            {
                "CUSTOMER_KEY": [-10, -10, -5, 30, 0, 1, 25],
                "ACCOUNT_BALANCE": [-101, -101, -51, 30, 0, 53, 105],
                "MARKET_SEGMENT": [
                    "AUTOMOBILE",
                    "AUTOMOBILE",
                    "FURNITURE",
                    "AUTOMOBILE",
                    "FURNITURE",
                    "MACHINERY",
                    "HOUSEHOLD",
                ],
            },
            index=native_pd.Index(
                ["i0", "i1", "i2", "i3", "i4", "i5", "i6"], name="index_name"
            ),
        )
    )


@pytest.fixture
def test_dfs_multiindex():
    return create_test_dfs(
        native_pd.DataFrame(
            {
                "CUSTOMER_KEY": [-10, -10, -5, 30, 0, 1, 25],
                "ACCOUNT_BALANCE": [-101, -101, -51, 30, 0, 53, 105],
                "MARKET_SEGMENT": [
                    "AUTOMOBILE",
                    "AUTOMOBILE",
                    "FURNITURE",
                    "AUTOMOBILE",
                    "FURNITURE",
                    "MACHINERY",
                    "HOUSEHOLD",
                ],
            },
            index=native_pd.MultiIndex.from_tuples(
                [
                    ("i00", "i01"),
                    ("i10", "i11"),
                    ("i20", "i21"),
                    (
                        "i30",
                        "i31",
                    ),
                    ("i40", "i41"),
                    ("i50", "i51"),
                    ("i60", "i61"),
                ],
                names=["level_0_name", "level_1_name"],
            ),
        )
    )
