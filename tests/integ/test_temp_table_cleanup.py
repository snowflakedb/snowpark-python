#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import copy
import gc
import logging
import re

import pytest

from snowflake.snowpark import Session
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
    warning_dict,
)
from snowflake.snowpark.functions import col
from tests.utils import IS_IN_STORED_PROC  # noqa: F401

pytestmark = [
    pytest.mark.skipif(
        "IS_IN_STORED_PROC or config.getoption('local_testing_mode', default=False)",
        reason="caplog is not working in stored procedure and temp table cleaner is not working in local testing mode",
    ),
]


@pytest.fixture(autouse=True)
def setup(session):
    auto_clean_up_temp_table_enabled = session.auto_clean_up_temp_table_enabled
    session.auto_clean_up_temp_table_enabled = True
    yield
    session.auto_clean_up_temp_table_enabled = auto_clean_up_temp_table_enabled


def wait_for_drop_table_sql_done(session: Session, caplog, expect_drop: bool) -> None:
    # Loop through captured logs and search for the pattern
    pattern = r"Dropping .* with query id ([0-9a-f\-]+)"
    matches = []
    for record in caplog.records:
        match = re.search(pattern, record.message)
        if match:
            query_id = match.group(1)
            matches.append(query_id)

    if len(matches) == 0:
        if expect_drop:
            pytest.fail("No drop table sql found in logs")
        else:
            caplog.clear()
            return

    caplog.clear()
    for query_id in matches:
        async_job = session.create_async_job(query_id)
        # Wait for the async job to finish
        _ = async_job.result()


def test_basic(session, caplog):
    session._temp_table_auto_cleaner.ref_count_map.clear()
    df1 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"]).cache_result()
    table_name = df1.table_name
    table_ids = table_name.split(".")
    df1.collect()
    assert session._temp_table_auto_cleaner.ref_count_map[table_name] == 1
    assert session._temp_table_auto_cleaner.num_temp_tables_created == 1
    assert session._temp_table_auto_cleaner.num_temp_tables_cleaned == 0
    df2 = df1.select("*").filter(col("a") == 1)
    df2.collect()
    assert session._temp_table_auto_cleaner.ref_count_map[table_name] == 1
    assert session._temp_table_auto_cleaner.num_temp_tables_created == 1
    assert session._temp_table_auto_cleaner.num_temp_tables_cleaned == 0
    df3 = df1.union_all(df2)
    df3.collect()
    assert session._temp_table_auto_cleaner.ref_count_map[table_name] == 1
    assert session._temp_table_auto_cleaner.num_temp_tables_created == 1
    assert session._temp_table_auto_cleaner.num_temp_tables_cleaned == 0

    del df1
    gc.collect()
    wait_for_drop_table_sql_done(session, caplog, expect_drop=False)
    assert session._table_exists(table_ids)
    assert session._temp_table_auto_cleaner.ref_count_map[table_name] == 1
    assert session._temp_table_auto_cleaner.num_temp_tables_created == 1
    assert session._temp_table_auto_cleaner.num_temp_tables_cleaned == 0

    del df2
    gc.collect()
    wait_for_drop_table_sql_done(session, caplog, expect_drop=False)
    assert session._table_exists(table_ids)
    assert session._temp_table_auto_cleaner.ref_count_map[table_name] == 1
    assert session._temp_table_auto_cleaner.num_temp_tables_created == 1
    assert session._temp_table_auto_cleaner.num_temp_tables_cleaned == 0

    del df3
    gc.collect()
    wait_for_drop_table_sql_done(session, caplog, expect_drop=True)
    assert not session._table_exists(table_ids)
    assert session._temp_table_auto_cleaner.ref_count_map[table_name] == 0
    assert session._temp_table_auto_cleaner.num_temp_tables_created == 1
    assert session._temp_table_auto_cleaner.num_temp_tables_cleaned == 1


def test_function(session, caplog):
    session._temp_table_auto_cleaner.ref_count_map.clear()
    table_name = None

    def f(session: Session) -> None:
        df = session.create_dataframe(
            [[1, 2], [3, 4]], schema=["a", "b"]
        ).cache_result()
        nonlocal table_name
        table_name = df.table_name
        assert session._temp_table_auto_cleaner.ref_count_map[table_name] == 1
        assert session._temp_table_auto_cleaner.num_temp_tables_created == 1
        assert session._temp_table_auto_cleaner.num_temp_tables_cleaned == 0

    f(session)
    gc.collect()
    wait_for_drop_table_sql_done(session, caplog, expect_drop=True)
    assert not session._table_exists(table_name.split("."))
    assert session._temp_table_auto_cleaner.ref_count_map[table_name] == 0
    assert session._temp_table_auto_cleaner.num_temp_tables_created == 1
    assert session._temp_table_auto_cleaner.num_temp_tables_cleaned == 1


@pytest.mark.parametrize(
    "copy_function",
    [
        lambda x: copy.copy(x),
        lambda x: x.alias("alias"),
        lambda x: x.na.replace(1, 2, subset=[]),
    ],
)
def test_copy(session, copy_function, caplog):
    session._temp_table_auto_cleaner.ref_count_map.clear()
    df1 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"]).cache_result()
    table_name = df1.table_name
    table_ids = table_name.split(".")
    df1.collect()
    assert session._temp_table_auto_cleaner.ref_count_map[table_name] == 1
    assert session._temp_table_auto_cleaner.num_temp_tables_created == 1
    assert session._temp_table_auto_cleaner.num_temp_tables_cleaned == 0
    df2 = copy_function(df1).select("*").filter(col("a") == 1)
    df2.collect()
    assert session._temp_table_auto_cleaner.ref_count_map[table_name] == 2
    assert session._temp_table_auto_cleaner.num_temp_tables_created == 1
    assert session._temp_table_auto_cleaner.num_temp_tables_cleaned == 0

    del df1
    gc.collect()
    wait_for_drop_table_sql_done(session, caplog, expect_drop=False)
    assert session._table_exists(table_ids)
    assert session._temp_table_auto_cleaner.ref_count_map[table_name] == 1
    assert session._temp_table_auto_cleaner.num_temp_tables_created == 1
    assert session._temp_table_auto_cleaner.num_temp_tables_cleaned == 0

    del df2
    gc.collect()
    wait_for_drop_table_sql_done(session, caplog, expect_drop=True)
    assert not session._table_exists(table_ids)
    assert session._temp_table_auto_cleaner.ref_count_map[table_name] == 0
    assert session._temp_table_auto_cleaner.num_temp_tables_created == 1
    assert session._temp_table_auto_cleaner.num_temp_tables_cleaned == 1


def test_reference_count_map_multiple_sessions(db_parameters, session, caplog):
    session._temp_table_auto_cleaner.ref_count_map.clear()
    new_session = Session.builder.configs(db_parameters).create()
    new_session.auto_clean_up_temp_table_enabled = True
    try:
        df1 = session.create_dataframe(
            [[1, 2], [3, 4]], schema=["a", "b"]
        ).cache_result()
        table_name1 = df1.table_name
        table_ids1 = table_name1.split(".")
        assert session._temp_table_auto_cleaner.ref_count_map[table_name1] == 1
        assert session._temp_table_auto_cleaner.num_temp_tables_created == 1
        assert session._temp_table_auto_cleaner.num_temp_tables_cleaned == 0
        assert table_name1 not in new_session._temp_table_auto_cleaner.ref_count_map
        assert new_session._temp_table_auto_cleaner.num_temp_tables_created == 0
        assert new_session._temp_table_auto_cleaner.num_temp_tables_cleaned == 0
        df2 = new_session.create_dataframe(
            [[1, 2], [3, 4]], schema=["a", "b"]
        ).cache_result()
        table_name2 = df2.table_name
        table_ids2 = table_name2.split(".")
        assert table_name2 not in session._temp_table_auto_cleaner.ref_count_map
        assert session._temp_table_auto_cleaner.num_temp_tables_created == 1
        assert session._temp_table_auto_cleaner.num_temp_tables_cleaned == 0
        assert new_session._temp_table_auto_cleaner.ref_count_map[table_name2] == 1
        assert new_session._temp_table_auto_cleaner.num_temp_tables_created == 1
        assert new_session._temp_table_auto_cleaner.num_temp_tables_cleaned == 0

        del df1
        gc.collect()
        wait_for_drop_table_sql_done(session, caplog, expect_drop=True)
        assert not session._table_exists(table_ids1)
        assert new_session._table_exists(table_ids2)
        assert session._temp_table_auto_cleaner.ref_count_map[table_name1] == 0
        assert session._temp_table_auto_cleaner.num_temp_tables_created == 1
        assert session._temp_table_auto_cleaner.num_temp_tables_cleaned == 1
        assert table_name1 not in new_session._temp_table_auto_cleaner.ref_count_map
        assert new_session._temp_table_auto_cleaner.num_temp_tables_created == 1
        assert new_session._temp_table_auto_cleaner.num_temp_tables_cleaned == 0

        del df2
        gc.collect()
        wait_for_drop_table_sql_done(session, caplog, expect_drop=True)
        assert not new_session._table_exists(table_ids2)
        assert table_name2 not in session._temp_table_auto_cleaner.ref_count_map
        assert session._temp_table_auto_cleaner.num_temp_tables_created == 1
        assert session._temp_table_auto_cleaner.num_temp_tables_cleaned == 1
        assert new_session._temp_table_auto_cleaner.ref_count_map[table_name2] == 0
        assert new_session._temp_table_auto_cleaner.num_temp_tables_created == 1
        assert new_session._temp_table_auto_cleaner.num_temp_tables_cleaned == 1
    finally:
        new_session.close()


def test_save_as_table_no_drop(session, caplog):
    session._temp_table_auto_cleaner.ref_count_map.clear()

    def f(session: Session, temp_table_name: str) -> None:
        session.create_dataframe(
            [[1, 2], [3, 4]], schema=["a", "b"]
        ).write.save_as_table(temp_table_name, table_type="temp")
        assert temp_table_name not in session._temp_table_auto_cleaner.ref_count_map
        assert session._temp_table_auto_cleaner.num_temp_tables_created == 0
        assert session._temp_table_auto_cleaner.num_temp_tables_cleaned == 0

    temp_table_name = random_name_for_temp_object(TempObjectType.TABLE)
    f(session, temp_table_name)
    gc.collect()
    wait_for_drop_table_sql_done(session, caplog, expect_drop=False)
    assert session._table_exists([temp_table_name])


def test_session_close(db_parameters, caplog):
    with Session.builder.configs(db_parameters).create() as new_session:
        df = new_session.create_dataframe(
            [[1, 2], [3, 4]], schema=["a", "b"]
        ).cache_result()

    with caplog.at_level(logging.WARNING):
        del df
        gc.collect()
    assert "Failed to drop temp table" not in caplog.text


def test_auto_clean_up_temp_table_enabled_parameter(db_parameters, session, caplog):
    warning_dict.clear()
    with caplog.at_level(logging.WARNING):
        session.auto_clean_up_temp_table_enabled = False
    assert session.auto_clean_up_temp_table_enabled is False
    assert "auto_clean_up_temp_table_enabled is experimental" in caplog.text
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"]).cache_result()
    table_name = df.table_name
    table_ids = table_name.split(".")
    del df
    gc.collect()
    wait_for_drop_table_sql_done(session, caplog, expect_drop=False)
    assert session._table_exists(table_ids)
    assert session._temp_table_auto_cleaner.ref_count_map[table_name] == 0
    assert session._temp_table_auto_cleaner.num_temp_tables_created == 1
    assert session._temp_table_auto_cleaner.num_temp_tables_cleaned == 1
    session.auto_clean_up_temp_table_enabled = True
    assert session.auto_clean_up_temp_table_enabled is True

    with pytest.raises(
        ValueError,
        match="value for auto_clean_up_temp_table_enabled must be True or False!",
    ):
        session.auto_clean_up_temp_table_enabled = -1
