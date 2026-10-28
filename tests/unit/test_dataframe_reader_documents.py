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
from snowflake.snowpark._internal.telemetry import TelemetryField
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
        # A MagicMock, not a plain string: documents() reads read_type/parse_mode/...
        # off the options object for telemetry after the read is built.
        module.DocumentReaderOptions.from_reader_options.return_value = mock.MagicMock()
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


@pytest.mark.parametrize(
    "options",
    [{"schema": "invoice_no"}, {"schema": 42}, {"prompt": ""}, {"prompt": "   "}],
)
def test_documents_invalid_schema_or_prompt_raises_before_any_query(options):
    reader = DataFrameReader(_create_fake_session())
    for key, value in options.items():
        reader = reader.option(key, value)

    with pytest.raises(SnowparkDataframeReaderException) as exc_info:
        reader.documents("@stage/doc.pdf")
    assert exc_info.value.error_code == "1120"


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
            module.DocumentReaderOptions.from_reader_options.return_value = (
                mock.MagicMock()
            )
            reader.documents("@stage/doc.pdf")
    assert "in private preview since 1.56.0" in warn.call_args[0][1]


def test_documents_reads_through_the_reader_module():
    reader = DataFrameReader(_create_fake_session()).option("prompt", "p")
    with mock.patch("snowflake.snowpark.dataframe_reader.document_reader") as module:
        # A MagicMock, not a plain string: documents() reads read_type/parse_mode/...
        # off the options object for telemetry after the read is built.
        fake_options = mock.MagicMock()
        module.DocumentReaderOptions.from_reader_options.return_value = fake_options
        df = reader.documents("@stage/doc.pdf")
    module.DocumentReaderOptions.from_reader_options.assert_called_once_with(
        {"PROMPT": "p"}
    )
    module.read_documents.assert_called_once_with(
        reader._session, "@stage/doc.pdf", fake_options
    )
    assert df is module.read_documents.return_value


def _read_with_real_options(reader: DataFrameReader, path: str):
    """Runs documents() with a real DocumentReaderOptions (so read_type and the
    other derived fields reflect the actual user options) while stubbing out
    read_documents() itself, which needs a live session."""
    fake_df = mock.MagicMock()
    fake_df._select_statement = None
    with mock.patch(
        "snowflake.snowpark.dataframe_reader.document_reader.read_documents",
        return_value=fake_df,
    ):
        df = reader.documents(path)
    assert df is fake_df
    return fake_df._plan.api_calls[0]


class TestDocumentsApiCallTelemetry:
    def test_a_parse_read_carries_read_type_and_parse_shape(self):
        reader = DataFrameReader(_create_fake_session())
        api_call = _read_with_real_options(reader, "@stage/docs/")
        assert api_call[TelemetryField.NAME.value] == "DataFrameReader.documents"
        assert api_call[TelemetryField.KEY_READ_TYPE.value] == "parse"
        assert api_call[TelemetryField.KEY_PARSE_MODE.value] == "layout"
        assert api_call[TelemetryField.KEY_ROW_BOUNDARY.value] == "document"
        assert api_call[TelemetryField.KEY_EXTRACTION_ENGINE.value] == "ai_complete"
        assert api_call[TelemetryField.KEY_EXTRACT_IMAGES.value] is False

    def test_a_prompt_read_carries_read_type_prompt_and_no_prompt_text(self):
        reader = DataFrameReader(_create_fake_session()).option(
            "prompt", "Summarize this. Secret account number: 12345."
        )
        api_call = _read_with_real_options(reader, "@stage/docs/")
        assert api_call[TelemetryField.KEY_READ_TYPE.value] == "prompt"
        assert api_call[TelemetryField.KEY_PARSE_MODE.value] == "none"
        assert "Secret" not in str(api_call) and "12345" not in str(api_call)

    def test_a_schema_read_carries_read_type_schema_and_no_schema_contents(self):
        reader = DataFrameReader(_create_fake_session()).option(
            "schema", {"ssn": "What is the social security number?"}
        )
        api_call = _read_with_real_options(reader, "@stage/docs/")
        assert api_call[TelemetryField.KEY_READ_TYPE.value] == "schema"
        assert api_call[TelemetryField.KEY_PARSE_MODE.value] == "none"
        assert api_call[TelemetryField.KEY_EXTRACTION_ENGINE.value] == "ai_complete"
        assert "ssn" not in str(api_call) and "social security" not in str(api_call)


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
