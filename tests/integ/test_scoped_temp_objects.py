#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from contextlib import contextmanager
import pytest

from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)
from tests.utils import IS_IN_STORED_PROC


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="SQL query not supported",
    run=False,
)
def test_create_scoped_temp_objects_syntax(session):
    snowpark_temp_table_name = random_name_for_temp_object(TempObjectType.TABLE)
    session._run_query(
        f"create scoped temporary table {snowpark_temp_table_name} (col int)"
    )
    session._run_query(
        f"create scoped temporary view {random_name_for_temp_object(TempObjectType.VIEW)} as select * from {snowpark_temp_table_name}"
    )
    session._run_query(
        f"create scoped temporary stage {random_name_for_temp_object(TempObjectType.STAGE)}"
    )
    session._run_query(
        f"create scoped temporary function {random_name_for_temp_object(TempObjectType.FUNCTION)} (arg int) returns int"
    )
    session._run_query(
        f"create scoped temporary function {random_name_for_temp_object(TempObjectType.TABLE_FUNCTION)} (arg int) returns table(col int) as $$ select * from {snowpark_temp_table_name} $$"
    )
    session._run_query(
        f"create scoped temporary file format {random_name_for_temp_object(TempObjectType.FILE_FORMAT)} type = csv"
    )

    with pytest.raises(ProgrammingError) as exc:
        session._run_query(
            f"create scoped temporary procedure {random_name_for_temp_object(TempObjectType.PROCEDURE)} (arg int) returns int"
        )
    assert "Unsupported feature 'SCOPED_TEMPORARY'." in str(exc)

    # CREATE SCOPED TEMPORARY TABLE previously only worked if the name of the object began with
    # SNOWPARK_TEMP_. This was changed to only enforce the naming convention if the query is issued
    # from within a stored procedure.

    @contextmanager
    def cm():
        if IS_IN_STORED_PROC:
            with pytest.raises(ProgrammingError) as exc:
                yield
            assert "Unsupported feature 'SCOPED_TEMPORARY'." in str(exc)
        else:
            yield

    temp_table_name = "temp_table"
    with cm():
        session._run_query(f"create scoped temporary table {temp_table_name} (col int)")
    with cm():
        session._run_query(
            f"create scoped temporary view temp_view as select * from {temp_table_name}"
        )
    with cm():
        session._run_query("create scoped temporary stage temp_stage")
    with cm():
        session._run_query(
            "create scoped temporary function temp_function (arg int) returns int"
        )
    with cm():
        session._run_query(
            f"create scoped temporary function temp_table_function (arg int) returns table(col int) as $$ select col from {temp_table_name} $$"
        )
    with cm():
        session._run_query(
            "create scoped temporary file format temp_file_format type = csv"
        )
