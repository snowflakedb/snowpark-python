#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Unit coverage for DataFrame.ai AST emission of optional parameters.

Regular integ tests typically run with AST disabled, so the ``if _emit_ast:``
branches that encode return_error_details / scores / config / options and the
new multi_embed / redact / translate methods need explicit unit coverage.
"""

from snowflake.snowpark._internal.utils import AstFlagSource, set_ast_state


def test_dataframe_ai_ast_optional_params(session):
    """Exercise DataFrame.ai AST paths that are otherwise skipped when AST is off."""
    from snowflake.snowpark.functions import col, to_file

    original = session.ast_enabled
    set_ast_state(AstFlagSource.TEST, True)
    session.ast_enabled = True
    try:
        df = session.create_dataframe(
            [["hello world"], ["test text"]], schema=["text"], _emit_ast=True
        )
        emit = True

        # complete: return_error_details AST field
        out = df.ai.complete(
            prompt="Say hi to {t}",
            input_columns={"t": col("text")},
            model="llama3.1-8b",
            return_error_details=True,
            _emit_ast=emit,
        )
        assert out._ast_id is not None

        # classify: return_error_details AST field
        out = df.ai.classify(
            input_column="text",
            categories=["greeting", "other"],
            return_error_details=True,
            _emit_ast=emit,
        )
        assert out._ast_id is not None

        # sentiment: return_error_details AST field
        out = df.ai.sentiment(
            input_column="text",
            categories=["tone"],
            return_error_details=True,
            _emit_ast=emit,
        )
        assert out._ast_id is not None

        # count_tokens: options + return_error_details AST fields
        out = df.ai.count_tokens(
            model="llama3.1-70b",
            prompt="text",
            options={"temperature": 0.0},
            return_error_details=True,
            _emit_ast=emit,
        )
        assert out._ast_id is not None

        # extract: scores + config AST fields
        out = df.ai.extract(
            input_column="text",
            response_format={"name": "What is the name?"},
            scores=True,
            config={"scale_factor": 2.0},
            _emit_ast=emit,
        )
        assert out._ast_id is not None

        # multi_embed: full AST bind path including kwargs
        out = df.ai.multi_embed(
            input_column="text",
            model="twelvelabs-marengo-embed-3-0",
            start_sec=1.0,
            _emit_ast=emit,
        )
        assert out._ast_id is not None

        # redact: categories / mode / return_error_details AST fields
        out = df.ai.redact(
            input_column="text",
            categories=["NAME", "EMAIL"],
            mode="detect",
            return_error_details=True,
            _emit_ast=emit,
        )
        assert out._ast_id is not None

        # translate: source/target language + return_error_details AST fields
        out = df.ai.translate(
            input_column="text",
            source_language="en",
            target_language="de",
            return_error_details=True,
            _emit_ast=emit,
        )
        assert out._ast_id is not None

        # transcribe / parse_document: return_error_details AST fields
        file_df = session.create_dataframe(
            [["@mystage/audio.ogg"]], schema=["path"], _emit_ast=True
        )
        out = file_df.ai.transcribe(
            input_column=to_file(file_df["path"]),
            return_error_details=True,
            timestamp_granularity="word",
            _emit_ast=emit,
        )
        assert out._ast_id is not None

        out = file_df.ai.parse_document(
            input_column=to_file(file_df["path"]),
            return_error_details=True,
            mode="LAYOUT",
            _emit_ast=emit,
        )
        assert out._ast_id is not None
    finally:
        set_ast_state(AstFlagSource.TEST, original)
        session.ast_enabled = original
