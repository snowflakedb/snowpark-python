#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from contextlib import contextmanager
import sys
from typing import Any, Dict, Iterator

from snowflake.snowpark import Session


def set_up_test_session_parameters(session: Session, local_testing_mode: bool) -> None:
    if local_testing_mode:
        return

    session.sql(
        "ALTER SESSION SET ENABLE_DEFAULT_PYTHON_ARTIFACT_REPOSITORY = true"
    ).collect()
    session.sql(
        "alter session set ENABLE_EXTRACTION_PUSHDOWN_EXTERNAL_PARQUET_FOR_COPY_PHASE_I='Track';"
    ).collect()
    session.sql("alter session set ENABLE_ROW_ACCESS_POLICY=true").collect()
    if sys.version_info.major == 3 and sys.version_info.minor == 14:
        session.sql("alter session set ENABLE_PYTHON_3_14=true").collect()


@contextmanager
def create_session_for_test(
    db_parameters: Dict[str, Any], *, remove_schema: bool = False
) -> Iterator[Session]:
    builder = Session.builder.configs(db_parameters)
    if remove_schema:
        builder = builder._remove_config("schema")

    with builder.create() as session:
        set_up_test_session_parameters(
            session, bool(db_parameters.get("local_testing", False))
        )
        yield session
