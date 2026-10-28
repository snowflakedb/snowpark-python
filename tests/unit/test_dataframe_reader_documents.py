#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import pytest

import snowflake.snowpark.session
from snowflake.snowpark._internal.analyzer.analyzer import Analyzer
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlanBuilder
from snowflake.snowpark._internal.ast.batch import AstBatch
from snowflake.snowpark._internal.server_connection import ServerConnection
from snowflake.snowpark.dataframe_reader import DataFrameReader
from snowflake.snowpark.exceptions import SnowparkDataframeReaderException


def _create_fake_session():
    """Build a minimal fake session suitable for DataFrameReader unit tests --
    path/option validation happens before any query is built, so this never
    needs to actually resolve or run anything."""

    def nop(name):
        return name

    fake_session = mock.create_autospec(snowflake.snowpark.session.Session)
    fake_session.sql_simplifier_enabled = True
    fake_session._cte_optimization_enabled = False
    fake_session._query_compilation_stage_enabled = False
    fake_session._join_alias_fix = False
    fake_session._conn = mock.create_autospec(ServerConnection)
    fake_session._conn._thread_safe_session_enabled = True
    fake_session._plan_builder = SnowflakePlanBuilder(fake_session)
    fake_session._analyzer = Analyzer(fake_session)
    fake_session._use_scoped_temp_objects = True
    fake_session._ast_batch = mock.create_autospec(AstBatch)
    fake_session.get_fully_qualified_name_if_possible = nop
    return fake_session


def test_documents_non_stage_path_raises_before_any_query():
    reader = DataFrameReader(_create_fake_session())
    with pytest.raises(ValueError, match="invalid Snowflake stage location"):
        reader._documents("/local/path/doc.pdf")


@pytest.mark.parametrize(
    "options",
    [
        {"parse_mode": "bogus"},
        {"row_boundary": "chapter"},
        {"mode": "DROPMALFORMED"},
        {"extraction_engine": "ai_agent"},
    ],
)
def test_documents_invalid_option_raises_before_any_query(options):
    reader = DataFrameReader(_create_fake_session())
    for key, value in options.items():
        reader = reader.option(key, value)

    with pytest.raises(SnowparkDataframeReaderException) as exc_info:
        reader._documents("@stage/doc.pdf")
    assert exc_info.value.error_code == "1116"
