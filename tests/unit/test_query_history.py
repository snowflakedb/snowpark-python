#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os

import pytest

from snowflake.snowpark import QueryRecord
from snowflake.snowpark.query_history import _VscHistoryExporter


def test_query_record_repr():
    record = QueryRecord("fake_id", "fake_text")
    record_with_thread_id = QueryRecord("fake_id", "fake_text", thread_id=123)
    record_with_is_describe = QueryRecord("fake_id", "fake_text", is_describe=True)
    record_with_both = QueryRecord(
        "fake_id", "fake_text", is_describe=True, thread_id=123
    )

    assert record.__repr__() == "QueryRecord(query_id=fake_id, sql_text=fake_text)"
    assert (
        record_with_thread_id.__repr__()
        == "QueryRecord(query_id=fake_id, sql_text=fake_text, thread_id=123)"
    )
    assert (
        record_with_is_describe.__repr__()
        == "QueryRecord(query_id=fake_id, sql_text=fake_text, is_describe=True)"
    )
    assert (
        record_with_both.__repr__()
        == "QueryRecord(query_id=fake_id, sql_text=fake_text, is_describe=True, thread_id=123)"
    )


def test_vsc_history_exporter_creates_target_directory(tmp_path):
    target = tmp_path / "history"
    assert not target.exists()

    _VscHistoryExporter(str(target))

    assert target.is_dir()


def test_vsc_history_exporter_writes_empty_file_named_after_query_id(tmp_path):
    exporter = _VscHistoryExporter(str(tmp_path))

    exporter._notify(QueryRecord("query-1", "select 1"))
    exporter._notify(QueryRecord("query-2", "select 2"))

    # Files should exist.
    assert (tmp_path / "query-1").is_file()
    assert (tmp_path / "query-2").is_file()
    # File contents should be empty.
    assert (tmp_path / "query-1").read_bytes() == b""
    assert (tmp_path / "query-2").read_bytes() == b""


def test_vsc_history_exporter_skips_describe_queries(tmp_path):
    exporter = _VscHistoryExporter(str(tmp_path))

    exporter._notify(QueryRecord("describe-id", "describe foo", is_describe=True))

    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("query_id", ["", None])
def test_vsc_history_exporter_skips_records_without_query_id(tmp_path, query_id):
    exporter = _VscHistoryExporter(str(tmp_path))

    exporter._notify(QueryRecord(query_id, "select 1"))

    assert list(tmp_path.iterdir()) == []


def test_vsc_history_exporter_init_swallows_oserror(monkeypatch):
    def raise_oserror(*args, **kwargs):
        raise OSError("cannot create directory")

    monkeypatch.setattr(os, "makedirs", raise_oserror)

    # Session creation should still succeed even if VSC history dir cannot be created.
    _VscHistoryExporter("/some/unwritable/path")


def test_vsc_history_exporter_notify_swallows_oserror(tmp_path):
    target = tmp_path / "history"
    exporter = _VscHistoryExporter(str(target))
    target.rmdir()  # Removing the dir makes the per-query file write fail.

    # _notify should not throw error even if it fails to write to the VSC history dir.
    exporter._notify(QueryRecord("query-1", "select 1"))
    assert not target.exists()


def test_vsc_history_exporter_disables_when_dir_at_limit(tmp_path):
    # Pre-fill the directory to the default limit.
    for i in range(_VscHistoryExporter.DEFAULT_FILE_COUNT_LIMIT_AT_INIT):
        (tmp_path / f"existing-{i}").touch()

    exporter = _VscHistoryExporter(str(tmp_path))
    assert exporter._disabled is True

    # _notify is a pure no-op: no new file is written.
    exporter._notify(QueryRecord("query-1", "select 1"))
    assert not (tmp_path / "query-1").exists()


def test_vsc_history_exporter_enabled_when_dir_under_limit(tmp_path):
    (tmp_path / "existing").touch()

    exporter = _VscHistoryExporter(str(tmp_path))
    assert exporter._disabled is False

    # Existing behavior is preserved: the query file is written.
    exporter._notify(QueryRecord("query-1", "select 1"))
    assert (tmp_path / "query-1").is_file()


def test_vsc_history_exporter_env_var_overrides_limit(tmp_path, monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_SNOWPARK_VSC_QUERY_HISTORY_DIR_MAX_FILES", "1")
    (tmp_path / "existing").touch()  # Already at the overridden limit of 1.

    exporter = _VscHistoryExporter(str(tmp_path))
    assert exporter._disabled is True

    exporter._notify(QueryRecord("query-1", "select 1"))
    assert not (tmp_path / "query-1").exists()


def test_vsc_history_exporter_invalid_env_var_falls_back_to_default(
    tmp_path, monkeypatch
):
    monkeypatch.setenv(
        "SNOWFLAKE_SNOWPARK_VSC_QUERY_HISTORY_DIR_MAX_FILES", "not-an-int"
    )
    (tmp_path / "existing").touch()

    # Invalid value is ignored; default (well above 1) keeps the exporter enabled.
    exporter = _VscHistoryExporter(str(tmp_path))
    assert exporter._disabled is False

    exporter._notify(QueryRecord("query-1", "select 1"))
    assert (tmp_path / "query-1").is_file()


def test_vsc_history_exporter_invalid_env_var_still_caps_at_default(
    tmp_path, monkeypatch
):
    # Exercises the ValueError fallback: a non-int env value is ignored and the
    # default limit is used, so a dir already at the default is still disabled.
    monkeypatch.setenv(
        "SNOWFLAKE_SNOWPARK_VSC_QUERY_HISTORY_DIR_MAX_FILES", "not-an-int"
    )
    for i in range(_VscHistoryExporter.DEFAULT_FILE_COUNT_LIMIT_AT_INIT):
        (tmp_path / f"existing-{i}").touch()

    exporter = _VscHistoryExporter(str(tmp_path))
    assert exporter._disabled is True

    exporter._notify(QueryRecord("query-1", "select 1"))
    assert not (tmp_path / "query-1").exists()


def test_vsc_history_exporter_disabled_when_dir_count_fails(tmp_path, monkeypatch):
    # Exercises the OSError fallback around os.listdir: if the directory cannot
    # be inspected, the exporter errs on the side of caution and stays disabled.
    def raise_oserror(*args, **kwargs):
        raise OSError("cannot list directory")

    monkeypatch.setattr(os, "listdir", raise_oserror)

    exporter = _VscHistoryExporter(str(tmp_path))
    assert exporter._disabled is True

    # _notify is a pure no-op: no file is written.
    exporter._notify(QueryRecord("query-1", "select 1"))
    assert not (tmp_path / "query-1").exists()
