#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

from tests.integ.modin.utils import assert_frame_equal
from tests.integ.utils.sql_counter import SqlCounter


def test_json_normalize_basic():
    data = [
        {"id": 1, "name": {"first": "Coleen", "last": "Volk"}},
        {"name": {"given": "Mark", "family": "Regner"}},
        {"id": 2, "name": "Faye Raker"},
    ]

    with SqlCounter(query_count=1):
        assert_frame_equal(
            pd.json_normalize(data),
            native_pd.json_normalize(data),
            check_dtype=False,
        )


@pytest.mark.parametrize("max_level", [0, 1])
def test_json_normalize_max_level(max_level):
    data = [
        {
            "id": 1,
            "name": "Cole Volk",
            "fitness": {"height": 130, "weight": 60},
        },
        {"name": "Mark Reg", "fitness": {"height": 130, "weight": 60}},
        {
            "id": 2,
            "name": "Faye Raker",
            "fitness": {"height": 130, "weight": 60},
        },
    ]

    with SqlCounter(query_count=1):
        assert_frame_equal(
            pd.json_normalize(data=data, max_level=max_level),
            native_pd.json_normalize(data=data, max_level=max_level),
            check_dtype=False,
        )


def test_json_normalize_record_path_meta():
    data = [
        {
            "state": "Florida",
            "shortname": "FL",
            "info": {"governor": "Rick Scott"},
            "counties": [
                {"name": "Dade", "population": 12345},
                {"name": "Broward", "population": 40000},
                {"name": "Palm Beach", "population": 60000},
            ],
        },
        {
            "state": "Ohio",
            "shortname": "OH",
            "info": {"governor": "John Kasich"},
            "counties": [
                {"name": "Summit", "population": 1234},
                {"name": "Cuyahoga", "population": 1337},
            ],
        },
    ]

    with SqlCounter(query_count=1):
        assert_frame_equal(
            pd.json_normalize(
                data=data,
                record_path="counties",
                meta=["state", "shortname", ["info", "governor"]],
            ),
            native_pd.json_normalize(
                data=data,
                record_path="counties",
                meta=["state", "shortname", ["info", "governor"]],
            ),
            check_dtype=False,
        )


def test_json_normalize_record_prefix():
    data = {"A": [1, 2]}

    with SqlCounter(query_count=1):
        assert_frame_equal(
            pd.json_normalize(data=data, record_prefix="Prefix."),
            native_pd.json_normalize(data=data, record_prefix="Prefix."),
            check_dtype=False,
        )
