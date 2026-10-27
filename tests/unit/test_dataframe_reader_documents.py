#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import base64
from unittest import mock

import pytest

import snowflake.snowpark.session
from snowflake.snowpark._internal.analyzer.analyzer import Analyzer
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlanBuilder
from snowflake.snowpark._internal.ast.batch import AstBatch
from snowflake.snowpark._internal.proto.generated import ast_pb2 as proto
from snowflake.snowpark._internal.server_connection import ServerConnection
from snowflake.snowpark._internal.utils import AstFlagSource, set_ast_state
from snowflake.snowpark.dataframe_reader import DataFrameReader
from snowflake.snowpark.exceptions import SnowparkDataframeReaderException
from snowflake.snowpark.functions import col


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
        reader.documents("/local/path/doc.pdf")


@pytest.mark.parametrize(
    "options",
    [
        # Chosen by the reader, so these are not options at all and are ignored.
        {"mode": "DROPMALFORMED"},
        {"extraction_engine": "ai_agent"},
        {"model": "claude-sonnet-4-5"},
        {"bogus_option": "anything"},
    ],
)
def test_documents_options_chosen_by_the_reader_are_ignored(options):
    reader = DataFrameReader(_create_fake_session())
    for key, value in options.items():
        reader = reader.option(key, value)
    with mock.patch("snowflake.snowpark.dataframe_reader.document_reader") as module:
        module.DocumentReaderOptions.from_reader_options.return_value = "options"
        reader.documents("@stage/doc.pdf")
    module.read_documents.assert_called_once()


@pytest.mark.parametrize(
    "options",
    [{"parse_mode": "bogus"}, {"row_boundary": "chapter"}, {"parse_mode": "none"}],
)
def test_documents_invalid_parse_option_raises_before_any_query(options):
    reader = DataFrameReader(_create_fake_session())
    for key, value in options.items():
        reader = reader.option(key, value)

    with pytest.raises(SnowparkDataframeReaderException) as exc_info:
        reader.documents("@stage/doc.pdf")
    assert exc_info.value.error_code == "1116"


def test_documents_is_a_public_private_preview_method():
    doc = DataFrameReader.documents.__doc__
    assert "in private preview since 1.56.0" in doc
    assert "parse_mode" in doc and "row_boundary" in doc and "_document_error" in doc


def test_documents_warns_that_it_is_in_private_preview():
    reader = DataFrameReader(_create_fake_session())
    with mock.patch("snowflake.snowpark._internal.utils.warning") as warn:
        with mock.patch(
            "snowflake.snowpark.dataframe_reader.document_reader"
        ) as module:
            module.DocumentReaderOptions.from_reader_options.return_value = "options"
            reader.documents("@stage/doc.pdf")
    assert "in private preview since 1.56.0" in warn.call_args[0][1]


def test_documents_reads_through_the_reader_module():
    reader = DataFrameReader(_create_fake_session()).option("prompt", "p")
    with mock.patch("snowflake.snowpark.dataframe_reader.document_reader") as module:
        module.DocumentReaderOptions.from_reader_options.return_value = "options"
        df = reader.documents("@stage/doc.pdf")
    module.DocumentReaderOptions.from_reader_options.assert_called_once_with(
        {"PROMPT": "p"}
    )
    # The reader's internal queries are never recorded in the AST; documents() records
    # its own read_documents statement instead.
    module.read_documents.assert_called_once_with(
        reader._session, "@stage/doc.pdf", "options", _emit_ast=False
    )
    assert df is module.read_documents.return_value


def test_private_documents_alias_delegates_to_documents():
    reader = DataFrameReader(_create_fake_session())
    with mock.patch.object(DataFrameReader, "documents") as documents:
        result = reader._documents("@stage/doc.pdf")
    documents.assert_called_once_with("@stage/doc.pdf")
    assert result is documents.return_value


def test_documents_is_not_supported_in_local_testing(session):
    # Reading documents needs AI functions, a stage and a stored procedure, none of
    # which Local Testing has, so the read stops before any of them is attempted.
    with mock.patch(
        "snowflake.snowpark.dataframe_reader.document_reader.read_documents"
    ) as read_documents:
        with pytest.raises(NotImplementedError, match="DataFrameReader.documents"):
            session.read.documents("@stage/docs/")
    read_documents.assert_not_called()


def _bound_exprs(session):
    """The expression variant of every statement bound since the last flush."""
    _, batch = session._ast_batch.flush()
    request = proto.Request()
    request.ParseFromString(base64.b64decode(batch))
    return [
        stmt.bind.expr for stmt in request.body if stmt.WhichOneof("variant") == "bind"
    ]


@pytest.fixture
def ast_session(session):
    original = session.ast_enabled
    set_ast_state(AstFlagSource.TEST, True)
    session.ast_enabled = True
    # The Local Testing guard in documents() would stop the read; the AST is what is
    # under test here, with the read itself mocked out.
    session._conn._suppress_not_implemented_error = True
    try:
        yield session
    finally:
        session._conn._suppress_not_implemented_error = False
        set_ast_state(AstFlagSource.TEST, original)
        session.ast_enabled = original


def _fake_read(session):
    return session.create_dataframe([[1]], schema=["a"], _emit_ast=False)


@pytest.mark.parametrize(
    "options",
    [
        {},
        {"parse_mode": "ocr", "row_boundary": "page"},
        {"schema": {"type": "object", "properties": {"a": {"type": "string"}}}},
    ],
)
def test_documents_records_one_read_documents_statement(ast_session, options):
    reader = ast_session.read
    for key, value in options.items():
        reader = reader.option(key, value)
    _bound_exprs(ast_session)  # start from an empty batch
    with mock.patch(
        "snowflake.snowpark.dataframe_reader.document_reader.read_documents",
        return_value=_fake_read(ast_session),
    ) as read_documents:
        df = reader.documents("@stage/docs/")

    assert read_documents.call_args.kwargs == {"_emit_ast": False}
    exprs = _bound_exprs(ast_session)
    assert [e.WhichOneof("variant") for e in exprs] == ["read_documents"]
    node = exprs[0].read_documents
    assert node.path == "@stage/docs/"
    assert [t._1 for t in node.reader.dataframe_reader.options] == list(options)
    assert df._ast_id is not None


def test_documents_result_can_be_chained_with_ast_on(ast_session):
    with mock.patch(
        "snowflake.snowpark.dataframe_reader.document_reader.read_documents",
        return_value=_fake_read(ast_session),
    ):
        df = ast_session.read.documents("@stage/docs/")
    chained = df.select("a").filter(col("a") > 0)
    assert chained._ast_id is not None
    kinds = [e.WhichOneof("variant") for e in _bound_exprs(ast_session)]
    assert kinds[0] == "read_documents"
    assert "dataframe_select" in kinds and "dataframe_filter" in kinds
