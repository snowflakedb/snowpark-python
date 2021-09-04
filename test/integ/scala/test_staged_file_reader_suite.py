#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

import pytest

from snowflake.snowpark.internal.analyzer.staged_file_reader import (
    StagedFileReader,
)
from snowflake.snowpark.snowpark_client_exception import SnowparkClientException
from snowflake.snowpark.types.sf_types import StructType, StructField, IntegerType, \
    StringType, DoubleType
from utils import Utils


def test_file_format_type(session_cnx):
    with session_cnx() as session:
        reader = StagedFileReader.from_session(session)

        assert reader.format("csv").format_type == "CSV"
        assert reader.format("CSV").format_type == "CSV"
        assert reader.format("json").format_type == "JSON"
        assert reader.format("Json").format_type == "JSON"
        assert reader.format("parquet").format_type == "PARQUET"
        assert reader.format("Parquet").format_type == "PARQUET"
        assert reader.format("avro").format_type == "AVRO"
        assert reader.format("AVRO").format_type == "AVRO"
        assert reader.format("ORC").format_type == "ORC"
        assert reader.format("orc").format_type == "ORC"
        assert reader.format("Xml").format_type == "XML"
        assert reader.format("XML").format_type == "XML"

        with pytest.raises(SnowparkClientException) as ex_info:
            reader.format("unknown_type")
        assert "Internal error: unsupported file format type: 'unknown_type'." in str(
            ex_info
        )


def test_option(session_cnx):
    with session_cnx() as session:
        reader = StagedFileReader.from_session(session)

        configs = {
            "Boolean": True,
            "int": 123,
            "Integer": 1,
            "true": "true",
            "false": "false",
            "string": "string",
        }
        saved_options = reader.options(configs)._StagedFileReader__cur_options
        assert len(saved_options) == 6
        assert saved_options["BOOLEAN"] == "True"
        assert saved_options["INT"] == "123"
        assert saved_options["INTEGER"] == "1"
        assert saved_options["TRUE"] == "true"
        assert saved_options["FALSE"] == "false"
        assert saved_options["STRING"] == "'string'"


def test_create_plan_for_copy(session_cnx):
    stage_location = "@myStage/prefix1/prefix2"

    user_schema = StructType(
        [
            StructField("a", IntegerType()),
            StructField("b", StringType()),
            StructField("c", DoubleType()),
        ]
    )

    with session_cnx() as session:
        reader = StagedFileReader.from_session(session).user_schema(user_schema).path(stage_location).option("SKIP_HEADER", 10).options({"DATE_FORMAT":"YYYY-MM-DD"}).format("csv").table('test_table_name')

        plan = reader.create_snowflake_plan()
        assert len(plan.queries) == 2
        crt = plan.queries[0].sql
        assert Utils.contain_ignore_case_and_whitespace(crt, "create table test_table_name if not exists")
        copy = plan.queries[-1].sql
        assert Utils.contain_ignore_case_and_whitespace(copy, "copy into test_table_name")
        assert Utils.contain_ignore_case_and_whitespace(copy, "skip_header = 10")
        assert Utils.contain_ignore_case_and_whitespace(copy, "DATE_FORMAT = 'YYYY-MM-DD'")
        assert Utils.contain_ignore_case_and_whitespace(copy, "TYPE = CSV")
