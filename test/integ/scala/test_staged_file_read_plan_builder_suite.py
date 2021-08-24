#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

import pytest

from snowflake.snowpark.internal.analyzer.staged_file_read_plan_builder import (
    StagedFileReadPlanBuilder,
)
from snowflake.snowpark.snowpark_client_exception import SnowparkClientException


def test_file_format_type(session_cnx):
    with session_cnx() as session:
        builder = StagedFileReadPlanBuilder(session)

        assert builder.format("csv").format_type == "CSV"
        assert builder.format("CSV").format_type == "CSV"
        assert builder.format("json").format_type == "JSON"
        assert builder.format("Json").format_type == "JSON"
        assert builder.format("parquet").format_type == "PARQUET"
        assert builder.format("Parquet").format_type == "PARQUET"
        assert builder.format("avro").format_type == "AVRO"
        assert builder.format("AVRO").format_type == "AVRO"
        assert builder.format("ORC").format_type == "ORC"
        assert builder.format("orc").format_type == "ORC"
        assert builder.format("Xml").format_type == "XML"
        assert builder.format("XML").format_type == "XML"

        with pytest.raises(SnowparkClientException) as ex_info:
            builder.format("unknown_type")
        assert "Internal error: unsupported file format type: 'unknown_type'." in str(
            ex_info
        )


def test_option(session_cnx):
    with session_cnx() as session:
        builder = StagedFileReadPlanBuilder(session)

        configs = {
            "Boolean": True,
            "int": 123,
            "Integer": 1,
            "true": "true",
            "false": "false",
            "string": "string",
        }
        saved_options = builder.options(configs)._StagedFileReadPlanBuilder__cur_options
        assert len(saved_options) == 6
        assert saved_options["BOOLEAN"] == "True"
        assert saved_options["INT"] == "123"
        assert saved_options["INTEGER"] == "1"
        assert saved_options["TRUE"] == "true"
        assert saved_options["FALSE"] == "false"
        assert saved_options["STRING"] == "'string'"
