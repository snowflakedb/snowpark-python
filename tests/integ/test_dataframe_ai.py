#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest
from snowflake.snowpark.functions import col


pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="AI functions are not yet supported in local testing mode.",
    ),
]


def test_dataframe_ai_complete_with_named_placeholders(session):
    """Test DataFrame.ai_complete with named placeholders."""
    # Create a DataFrame with review data
    df = session.create_dataframe(
        [
            ["Great product, highly recommend!", 5, "electronics"],
            ["Poor quality, very disappointed", 1, "clothing"],
            ["Average product, nothing special", 3, "books"],
        ],
        schema=["review", "rating", "category"],
    )

    # Use DataFrame.ai_complete with named placeholders
    result_df = df.ai_complete(
        prompt="Analyze this {category} product review: '{review}' with rating {rating}/5. What is the sentiment?",
        input_columns={
            "review": col("review"),
            "rating": col("rating"),
            "category": col("category"),
        },
        output_column="sentiment_analysis",
        model="snowflake-arctic",
    )

    # Check schema
    assert result_df.columns == ["REVIEW", "RATING", "CATEGORY", "SENTIMENT_ANALYSIS"]

    # Collect and verify results
    results = result_df.collect()
    assert len(results) == 3

    for row in results:
        assert row["SENTIMENT_ANALYSIS"] is not None
        assert len(row["SENTIMENT_ANALYSIS"]) > 0


def test_dataframe_ai_complete_with_positional_placeholders(session):
    """Test DataFrame.ai_complete with positional placeholders."""
    # Create a DataFrame
    df = session.create_dataframe(
        [
            ["Python", "programming"],
            ["SQL", "database"],
            ["Machine Learning", "AI"],
        ],
        schema=["topic", "category"],
    )

    # Use DataFrame.ai_complete with positional placeholders
    result_df = df.ai_complete(
        prompt="Define {0} in the context of {1} in one sentence.",
        input_columns=[col("topic"), col("category")],
        output_column="definition",
        model="claude-4-sonnet",
    )

    # Check schema
    assert result_df.columns == ["TOPIC", "CATEGORY", "DEFINITION"]

    # Collect and verify results
    results = result_df.collect()
    assert len(results) == 3

    for row in results:
        assert row["DEFINITION"] is not None
        assert len(row["DEFINITION"]) > 10


def test_dataframe_ai_complete_default_output_column(session):
    """Test DataFrame.ai_complete with default output column name."""
    df = session.create_dataframe(
        [["What is 2+2?"], ["What is the capital of France?"]], schema=["question"]
    )

    # Don't specify output_column, should use default
    result_df = df.ai_complete(
        prompt="Answer the question",
        input_columns=[col("question")],
        model="snowflake-arctic",
        model_parameters={
            "temperature": 0.8,
            "top_p": 0.95,
            "max_tokens": 100,
            "guardrails": False,
        },
    )

    # Check that default column name is used
    assert "AI_COMPLETE_OUTPUT" in result_df.schema.names

    results = result_df.collect()
    assert len(results) == 2
    for row in results:
        assert row["AI_COMPLETE_OUTPUT"] is not None


def test_dataframe_ai_complete_error_handling(session):
    """Test error handling in DataFrame.ai_complete."""

    # Test missing model parameter
    df = session.create_dataframe([["test"]], schema=["text"])
    with pytest.raises(ValueError, match="model must be specified"):
        df.ai_complete(
            prompt="Test {text}",
            input_columns={"text": col("text")}
            # model parameter missing
        )

    # Test invalid input_columns type
    with pytest.raises(
        TypeError, match="input_columns must be a list of Columns or a dict"
    ):
        df.ai_complete(
            prompt="Test",
            input_columns="invalid",  # Should be list or dict
            model="snowflake-arctic",
        )
