#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
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

    temp_table_name = "temp_table"
    with pytest.raises(ProgrammingError) as exc:
        session._run_query(f"create scoped temporary table {temp_table_name} (col int)")
    assert "Unsupported feature 'SCOPED_TEMPORARY'." in str(exc)
    with pytest.raises(ProgrammingError) as exc:
        session._run_query(
            f"create scoped temporary view temp_view as select * from {temp_table_name}"
        )
    assert "Unsupported feature 'SCOPED_TEMPORARY'." in str(exc)
    with pytest.raises(ProgrammingError) as exc:
        session._run_query("create scoped temporary stage temp_stage")
    assert "Unsupported feature 'SCOPED_TEMPORARY'." in str(exc)
    with pytest.raises(ProgrammingError) as exc:
        session._run_query(
            "create scoped temporary function temp_function (arg int) returns int"
        )
    assert "Unsupported feature 'SCOPED_TEMPORARY'." in str(exc)
    with pytest.raises(ProgrammingError) as exc:
        session._run_query(
            "create scoped temporary function temp_talbe_function (arg int) returns table(col int) as $$ select * from {temp_table_name} $$"
        )
    assert "Unsupported feature 'SCOPED_TEMPORARY'." in str(exc)
    with pytest.raises(ProgrammingError) as exc:
        session._run_query(
            "create scoped temporary file format temp_file_format type = csv"
        )
    assert "Unsupported feature 'SCOPED_TEMPORARY'." in str(exc)
