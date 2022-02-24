#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import os.path

import pytest

from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark.functions import col
from tests.utils import TYPE_MAP, TempObjectType, TestFiles, Utils

tmp_stage_name = Utils.random_name_for_temp_object(TempObjectType.STAGE)
tmp_table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
tmp_full_types_table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
tmp_full_types_table_name2 = Utils.random_name_for_temp_object(TempObjectType.TABLE)


@pytest.fixture(scope="module", autouse=True)
def setup(session):
    Utils.create_stage(session, tmp_stage_name, is_temporary=True)
    Utils.create_table(session, tmp_table_name, schema="num int", is_temporary=True)
    Utils.create_table(
        session,
        tmp_full_types_table_name,
        schema=",".join(
            [
                f'{row.col_name} {row.sf_type},"{row.col_name}" {row.sf_type} not null'
                for row in TYPE_MAP
            ]
        ),
        is_temporary=True,
    )
    Utils.create_table(
        session,
        tmp_full_types_table_name2,
        schema=",".join([f"{row.col_name} {row.sf_type}" for row in TYPE_MAP]),
        is_temporary=True,
    )


def test_show_database_objects(session):
    sqls = [
        "show schemas",
        "show objects",
        "show external tables",
        "show views",
        "show columns",
        "show file formats",
        "show sequences",
        "show stages",
        "show pipes",
        "show streams",
        "show tasks",
        "show user functions",
        "show external functions",
        "show procedures",
        "show tables",
        "show parameters",
        "show shares",
        "show warehouses",
        "show transactions",
        "show locks",
        "show regions",
    ]
    for sql in sqls:
        Utils.verify_schema(sql, session.sql(sql).schema, session)


def test_show_account_objects(session):
    sqls = ["show functions", "show network policies", "show roles", "show databases"]
    for sql in sqls:
        Utils.verify_schema(sql, session.sql(sql).schema, session)


def test_show_session_operations(session):
    sql = "show variables"
    Utils.verify_schema(sql, session.sql(sql).schema, session)


def test_show_terse(session):
    sqls = [
        "show terse databases",
        "show terse schemas",
        "show terse tables",
        "show terse views",
        "show terse streams",
        "show terse tasks",
        "show terse external tables",
    ]
    for sql in sqls:
        Utils.verify_schema(sql, session.sql(sql).schema, session)


def test_invalid_show_commands(session):
    with pytest.raises(ProgrammingError):
        session.sql("show").schema
    with pytest.raises(ProgrammingError):
        session.sql("show table").schema
    with pytest.raises(ProgrammingError):
        session.sql("show tables abc").schema
    with pytest.raises(ProgrammingError):
        session.sql("show external abc").schema


def test_alter(session):
    sql = "alter session set ABORT_DETACHED_QUERY=false"
    Utils.verify_schema(sql, session.sql(sql).schema, session)


def test_list_remove_file(session, resources_path):
    test_files = TestFiles(resources_path)

    sqls = [f"ls @{tmp_stage_name}", f"list @{tmp_stage_name}"]
    for sql in sqls:
        Utils.verify_schema(sql, session.sql(sql).schema, session)

    Utils.upload_to_stage(
        session, tmp_stage_name, test_files.test_file_csv, compress=False
    )
    Utils.upload_to_stage(
        session, tmp_stage_name, test_files.test_file2_csv, compress=False
    )

    Utils.verify_schema(
        f"rm @{tmp_stage_name}/{os.path.basename(test_files.test_file_csv)}",
        session.sql(
            f"rm @{tmp_stage_name}/{os.path.basename(test_files.test_file2_csv)}"
        ).schema,
        session,
    )

    Utils.upload_to_stage(
        session, tmp_stage_name, test_files.test_file_csv, compress=False
    )
    Utils.upload_to_stage(
        session, tmp_stage_name, test_files.test_file2_csv, compress=False
    )

    Utils.verify_schema(
        f"rm @{tmp_stage_name}/{os.path.basename(test_files.test_file_csv)}",
        session.sql(
            f"rm @{tmp_stage_name}/{os.path.basename(test_files.test_file2_csv)}"
        ).schema,
        session,
    )


def test_insert_into(session):
    sql = f"insert into {tmp_table_name} values(1)"
    Utils.verify_schema(sql, session.sql(sql).schema, session)


def test_delete(session):
    sql = f"delete from {tmp_table_name} where num = 1"
    Utils.verify_schema(sql, session.sql(sql).schema, session)


def test_update(session):
    sql = f"update {tmp_table_name} set num = 2 where num = 1"
    Utils.verify_schema(sql, session.sql(sql).schema, session)


def test_select(session):
    sql = f"select * from {tmp_table_name}"
    Utils.verify_schema(sql, session.sql(sql).schema, session)


def test_analyze_and_verify_full_schema(session):
    Utils.verify_schema(
        f"select * from {tmp_full_types_table_name}",
        session.table(tmp_full_types_table_name).schema,
        session,
    )
    Utils.verify_schema(
        f"select * from {tmp_full_types_table_name2}",
        session.table(tmp_full_types_table_name2).schema,
        session,
    )
    table = session.table(tmp_full_types_table_name)
    df1 = table.select("string", '"int"', "array", '"date"')
    Utils.verify_schema(
        f'select string, "int", array, "date" from {tmp_full_types_table_name}',
        df1.schema,
        session,
    )
    df2 = df1.filter(col('"int"') > 0)
    Utils.verify_schema(
        f'select string, "int", array, "date" from {tmp_full_types_table_name} where "int" > 0',
        df2.schema,
        session,
    )


def test_verify_time_schema_type(session):
    Utils.verify_schema(
        f"select time from {tmp_full_types_table_name2}",
        session.table(tmp_full_types_table_name2).select("time").schema,
        session,
    )


def test_use(session):
    current_db = session.get_current_database()
    current_schema = session.get_current_schema()
    sqls = [f"use database {current_db}", f"use schema {current_schema}"]
    for sql in sqls:
        Utils.verify_schema(sql, session.sql(sql).schema, session)


def test_create_drop(session):
    tmp_table_name1 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    tmp_stage_name1 = Utils.random_name_for_temp_object(TempObjectType.STAGE)
    tmp_stream_name = f"stream_{Utils.random_alphanumeric_str(10)}"
    tmp_view_name = Utils.random_name_for_temp_object(TempObjectType.VIEW)
    tmp_pipe_name = f"pipe_{Utils.random_alphanumeric_str(10)}"

    try:
        sql = f"create or replace table {tmp_table_name1} (num int)"
        Utils.verify_schema(sql, session.sql(sql).schema, session)
        sql = f"drop table {tmp_table_name1}"
        Utils.verify_schema(sql, session.sql(sql).schema, session)

        session._run_query(f"create or replace table {tmp_table_name1} (num int)")
        sql = f"create or replace stream {tmp_stream_name} on table {tmp_table_name1}"
        Utils.verify_schema(sql, session.sql(sql).schema, session)
        sql = f"drop stream {tmp_stream_name}"
        Utils.verify_schema(sql, session.sql(sql).schema, session)

        sql = f"create or replace stage {tmp_stage_name1}"
        Utils.verify_schema(sql, session.sql(sql).schema, session)
        sql = f"drop stage {tmp_stage_name1}"
        Utils.verify_schema(sql, session.sql(sql).schema, session)

        sql = (
            f"create or replace view {tmp_view_name} as select * from {tmp_table_name1}"
        )
        Utils.verify_schema(sql, session.sql(sql).schema, session)
        sql = f"drop view {tmp_view_name}"
        Utils.verify_schema(sql, session.sql(sql).schema, session)

        session._run_query(f"create or replace stage {tmp_stage_name1}")
        sql = f"create or replace pipe {tmp_pipe_name} as copy into {tmp_table_name1} from @{tmp_stage_name1}"
        Utils.verify_schema(sql, session.sql(sql).schema, session)
        sql = f"drop pipe {tmp_pipe_name}"
        Utils.verify_schema(sql, session.sql(sql).schema, session)
    finally:
        session._run_query(f"drop table if exists {tmp_table_name1}")
        session._run_query(f"drop stage if exists {tmp_stage_name1}")
        session._run_query(f"drop stream if exists {tmp_stream_name}")
        session._run_query(f"drop view if exists {tmp_view_name}")
        session._run_query(f"drop pipe if exists {tmp_pipe_name}")


def test_comment_on(session):
    tmp_table_name1 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(session, tmp_table_name1, "num int", is_temporary=True)
    sql = f"comment on table {tmp_table_name1} is 'test'"
    Utils.verify_schema(sql, session.sql(sql).schema, session)


def test_grant_revoke(session):
    tmp_table_name1 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(session, tmp_table_name1, "num int", is_temporary=True)
    current_role = session.get_current_role()

    sql = f"grant select on table {tmp_table_name1} to role {current_role}"
    Utils.verify_schema(sql, session.sql(sql).schema, session)

    sql = f"revoke select on table {tmp_table_name1} from role {current_role}"
    Utils.verify_schema(sql, session.sql(sql).schema, session)


def test_describe(session):
    tmp_table_name1 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    tmp_stage_name1 = Utils.random_name_for_temp_object(TempObjectType.STAGE)
    tmp_stream_name = f"stream_{Utils.random_alphanumeric_str(10)}"
    tmp_view_name = Utils.random_name_for_temp_object(TempObjectType.VIEW)
    tmp_pipe_name = f"pipe_{Utils.random_alphanumeric_str(10)}"

    try:
        session._run_query(f"create or replace table {tmp_table_name1} (num int)")
        sql = f"describe table {tmp_table_name1}"
        Utils.verify_schema(sql, session.sql(sql).schema, session)

        session._run_query(
            f"create or replace stream {tmp_stream_name} on table {tmp_table_name1}"
        )
        sql = f"describe stream {tmp_stream_name}"
        Utils.verify_schema(sql, session.sql(sql).schema, session)

        session._run_query(f"create or replace stage {tmp_stage_name1}")
        sql = f"describe stage {tmp_stage_name1}"
        Utils.verify_schema(sql, session.sql(sql).schema, session)

        session._run_query(
            f"create or replace view {tmp_view_name} as select * from {tmp_table_name1}"
        )
        sql = f"describe view {tmp_view_name}"
        Utils.verify_schema(sql, session.sql(sql).schema, session)

        session._run_query(
            f"create or replace pipe {tmp_pipe_name} as copy into {tmp_table_name1} from @{tmp_stage_name1}"
        )
        sql = f"describe pipe {tmp_pipe_name}"
        Utils.verify_schema(sql, session.sql(sql).schema, session)
    finally:
        session._run_query(f"drop table if exists {tmp_table_name1}")
        session._run_query(f"drop stage if exists {tmp_stage_name1}")
        session._run_query(f"drop stream if exists {tmp_stream_name}")
        session._run_query(f"drop view if exists {tmp_view_name}")
        session._run_query(f"drop pipe if exists {tmp_pipe_name}")
