#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import pytest

from snowflake.snowpark._internal.utils import TimeTravelConfig
from snowflake.snowpark.table import Table


def make_table(**time_travel_kwargs):
    """Build a Table without a connection, just enough for Table.sample's SQL path."""
    table = object.__new__(Table)
    table.table_name = "T"
    table._time_travel_config = TimeTravelConfig.validate_and_normalize_params(
        **time_travel_kwargs
    )
    table._iceberg_changes_config = None
    table._session = mock.MagicMock()
    table._session._conn = mock.MagicMock()
    table._session.reduce_describe_query_enabled = False
    table._plan = mock.MagicMock()
    return table


def sampled_sql(table, **kwargs):
    table.sample(_emit_ast=False, **kwargs)
    return table._session.sql.call_args.args[0]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"frac": 0.5},
        {"n": 3},
        {"frac": 0.5, "seed": 1},
        {"frac": 0.5, "sampling_method": "ROW"},
        {"frac": 0.5, "sampling_method": "SYSTEM"},
        {"frac": 0.5, "sampling_method": "block"},
    ],
)
@pytest.mark.parametrize(
    "time_travel_kwargs",
    [
        {"changes": "default", "time_travel_mode": "at", "statement": "q1"},
        {"changes": "append_only", "time_travel_mode": "before", "offset": -60},
    ],
)
def test_sample_not_supported_for_changes(time_travel_kwargs, kwargs):
    table = make_table(**time_travel_kwargs)
    with pytest.raises(ValueError, match="not supported for change-tracking reads"):
        table.sample(_emit_ast=False, **kwargs)
    table._session.sql.assert_not_called()


def test_sample_seed_still_allowed_for_plain_time_travel():
    table = make_table(time_travel_mode="at", statement="q1")
    sql = sampled_sql(table, frac=0.5, seed=3)
    assert "AT (STATEMENT => 'q1') SAMPLE" in sql and "SEED (3)" in sql


def test_sample_keeps_iceberg_version_time_travel():
    table = make_table(time_travel_mode="at", version=1234567890)
    assert sampled_sql(table, frac=0.5).startswith(
        "SELECT * FROM T AT (VERSION => 1234567890) SAMPLE  (50.0)"
    )
    sql = sampled_sql(table, n=2, seed=7)
    assert "FROM T AT (VERSION => 1234567890) SAMPLE  (2 ROWS)  SEED (7)" in sql


@pytest.mark.parametrize("arg", ["version_ref", "branch", "version_tag"])
def test_sample_keeps_iceberg_version_ref_time_travel(arg):
    table = make_table(time_travel_mode="at", **{arg: "audit-branch"})
    assert sampled_sql(table, frac=0.5).startswith(
        "SELECT * FROM T AT (VERSION_REF => 'audit-branch') SAMPLE  (50.0)"
    )
