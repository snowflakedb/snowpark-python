#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import copy
import gc
import logging
import time

import pytest

from snowflake.snowpark import Session
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.functions import col
from tests.utils import IS_IN_STORED_PROC

pytestmark = [
    pytest.mark.skip(
        reason="SNOW-1645523: Re-enable this test file after flaky test is fixed",
    ),
]

WAIT_TIME = 1


def test_basic(session):
    df1 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"]).cache_result()
    table_name = df1.table_name
    table_ids = table_name.split(".")
    df1.collect()
    assert session._temp_table_auto_cleaner.ref_count_map[table_name] == 1
    df2 = df1.select("*").filter(col("a") == 1)
    df2.collect()
    assert session._temp_table_auto_cleaner.ref_count_map[table_name] == 1
    df3 = df1.union_all(df2)
    df3.collect()
    assert session._temp_table_auto_cleaner.ref_count_map[table_name] == 1

    session._temp_table_auto_cleaner.start()
    del df1
    gc.collect()
    time.sleep(WAIT_TIME)
    assert session._table_exists(table_ids)
    assert session._temp_table_auto_cleaner.ref_count_map[table_name] == 1

    del df2
    gc.collect()
    time.sleep(WAIT_TIME)
    assert session._table_exists(table_ids)
    assert session._temp_table_auto_cleaner.ref_count_map[table_name] == 1

    del df3
    gc.collect()
    time.sleep(WAIT_TIME)
    assert not session._table_exists(table_ids)
    assert table_name not in session._temp_table_auto_cleaner.ref_count_map


def test_function(session):
    table_name = None

    def f(session: Session) -> None:
        df = session.create_dataframe(
            [[1, 2], [3, 4]], schema=["a", "b"]
        ).cache_result()
        nonlocal table_name
        table_name = df.table_name
        assert session._temp_table_auto_cleaner.ref_count_map[table_name] == 1

    session._temp_table_auto_cleaner.start()
    f(session)
    gc.collect()
    time.sleep(WAIT_TIME)
    assert not session._table_exists(table_name.split("."))
    assert session._temp_table_auto_cleaner.ref_count_map[table_name] == 0


@pytest.mark.parametrize(
    "copy_function",
    [
        lambda x: copy.copy(x),
        lambda x: x.alias("alias"),
        lambda x: x.na.replace(1, 2, subset=[]),
    ],
)
def test_copy(session, copy_function):
    df1 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"]).cache_result()
    table_name = df1.table_name
    table_ids = table_name.split(".")
    df1.collect()
    assert session._temp_table_auto_cleaner.ref_count_map[table_name] == 1
    df2 = copy_function(df1).select("*").filter(col("a") == 1)
    df2.collect()
    assert session._temp_table_auto_cleaner.ref_count_map[table_name] == 2

    session._temp_table_auto_cleaner.start()
    del df1
    gc.collect()
    time.sleep(WAIT_TIME)
    assert session._table_exists(table_ids)
    assert session._temp_table_auto_cleaner.ref_count_map[table_name] == 1

    session._temp_table_auto_cleaner.start()
    del df2
    gc.collect()
    time.sleep(WAIT_TIME)
    assert not session._table_exists(table_ids)
    assert table_name not in session._temp_table_auto_cleaner.ref_count_map


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Cannot create session in SP")
def test_reference_count_map_multiple_sessions(db_parameters, session):
    new_session = Session.builder.configs(db_parameters).create()
    try:
        df1 = session.create_dataframe(
            [[1, 2], [3, 4]], schema=["a", "b"]
        ).cache_result()
        table_name1 = df1.table_name
        table_ids1 = table_name1.split(".")
        assert session._temp_table_auto_cleaner.ref_count_map[table_name1] == 1
        assert new_session._temp_table_auto_cleaner.ref_count_map[table_name1] == 0
        df2 = new_session.create_dataframe(
            [[1, 2], [3, 4]], schema=["a", "b"]
        ).cache_result()
        table_name2 = df2.table_name
        table_ids2 = table_name2.split(".")
        assert session._temp_table_auto_cleaner.ref_count_map[table_name2] == 0
        assert new_session._temp_table_auto_cleaner.ref_count_map[table_name2] == 1

        session._temp_table_auto_cleaner.start()
        del df1
        gc.collect()
        time.sleep(WAIT_TIME)
        assert not session._table_exists(table_ids1)
        assert new_session._table_exists(table_ids2)
        assert session._temp_table_auto_cleaner.ref_count_map[table_name1] == 0
        assert new_session._temp_table_auto_cleaner.ref_count_map[table_name1] == 0

        new_session._temp_table_auto_cleaner.start()
        del df2
        gc.collect()
        time.sleep(WAIT_TIME)
        assert not new_session._table_exists(table_ids2)
        assert session._temp_table_auto_cleaner.ref_count_map[table_name2] == 0
        assert new_session._temp_table_auto_cleaner.ref_count_map[table_name2] == 0
    finally:
        new_session.close()


def test_save_as_table_no_drop(session):
    session._temp_table_auto_cleaner.start()

    def f(session: Session, temp_table_name: str) -> None:
        session.create_dataframe(
            [[1, 2], [3, 4]], schema=["a", "b"]
        ).write.save_as_table(temp_table_name, table_type="temp")
        assert session._temp_table_auto_cleaner.ref_count_map[temp_table_name] == 0

    temp_table_name = random_name_for_temp_object(TempObjectType.TABLE)
    f(session, temp_table_name)
    gc.collect()
    time.sleep(WAIT_TIME)
    assert session._table_exists([temp_table_name])


def test_start_stop(session):
    session._temp_table_auto_cleaner.stop()

    df1 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"]).cache_result()
    table_name = df1.table_name
    table_ids = table_name.split(".")
    assert session._temp_table_auto_cleaner.ref_count_map[table_name] == 1
    del df1
    gc.collect()
    assert session._temp_table_auto_cleaner.ref_count_map[table_name] == 0
    assert not session._temp_table_auto_cleaner.queue.empty()
    assert session._table_exists(table_ids)

    session._temp_table_auto_cleaner.start()
    time.sleep(WAIT_TIME)
    assert session._temp_table_auto_cleaner.queue.empty()
    assert not session._table_exists(table_ids)


def test_auto_clean_up_temp_table_enabled_parameter(db_parameters, session, caplog):
    with caplog.at_level(logging.WARNING):
        session.auto_clean_up_temp_table_enabled = True
    assert session.auto_clean_up_temp_table_enabled is True
    assert "auto_clean_up_temp_table_enabled is experimental" in caplog.text
    assert session._temp_table_auto_cleaner.is_alive()
    session.auto_clean_up_temp_table_enabled = False
    assert session.auto_clean_up_temp_table_enabled is False
    assert not session._temp_table_auto_cleaner.is_alive()
    with pytest.raises(
        ValueError,
        match="value for auto_clean_up_temp_table_enabled must be True or False!",
    ):
        session.auto_clean_up_temp_table_enabled = -1
