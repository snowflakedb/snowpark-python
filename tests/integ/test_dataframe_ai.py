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
    """Test DataFrame.ai.complete with named placeholders."""
    # Create a DataFrame with review data
    df = session.create_dataframe(
        [
            ["Great product, highly recommend!", 5, "electronics"],
            ["Poor quality, very disappointed", 1, "clothing"],
            ["Average product, nothing special", 3, "books"],
        ],
        schema=["review", "rating", "category"],
    )

    # Use DataFrame.ai.complete with named placeholders
    result_df = df.ai.complete(
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
    results = result_df.collect(_emit_ast=False)
    assert len(results) == 3

    for row in results:
        assert row["SENTIMENT_ANALYSIS"] is not None
        assert len(row["SENTIMENT_ANALYSIS"]) > 0


def test_dataframe_ai_complete_with_positional_placeholders(session):
    """Test DataFrame.ai.complete with positional placeholders."""
    # Create a DataFrame
    df = session.create_dataframe(
        [
            ["Python", "programming"],
            ["SQL", "database"],
            ["Machine Learning", "AI"],
        ],
        schema=["topic", "category"],
    )

    # Use DataFrame.ai.complete with positional placeholders
    result_df = df.ai.complete(
        prompt="Define {0} in the context of {1} in one sentence.",
        input_columns=[col("topic"), col("category")],
        output_column="definition",
        model="claude-4-sonnet",
    )

    # Check schema
    assert result_df.columns == ["TOPIC", "CATEGORY", "DEFINITION"]

    # Collect and verify results
    results = result_df.collect(_emit_ast=False)
    assert len(results) == 3

    for row in results:
        assert row["DEFINITION"] is not None
        assert len(row["DEFINITION"]) > 10


def test_dataframe_ai_complete_default_output_column(session):
    """Test DataFrame.ai.complete with default output column name."""
    df = session.create_dataframe(
        [["What is 2+2?"], ["What is the capital of France?"]], schema=["question"]
    )

    # Don't specify output_column, should use default
    result_df = df.ai.complete(
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

    results = result_df.collect(_emit_ast=False)
    assert len(results) == 2
    for row in results:
        assert row["AI_COMPLETE_OUTPUT"] is not None


def test_dataframe_ai_complete_error_handling(session):
    """Test error handling in DataFrame.ai.complete."""

    # Test missing model parameter
    df = session.create_dataframe([["test"]], schema=["text"])
    with pytest.raises(ValueError, match="model must be specified"):
        df.ai.complete(
            prompt="Test {text}",
            input_columns={"text": col("text")}
            # model parameter missing
        )

    # Test invalid input_columns type
    with pytest.raises(
        TypeError, match="input_columns must be a list of Columns or a dict"
    ):
        df.ai.complete(
            prompt="Test",
            input_columns="invalid",  # Should be list or dict
            model="snowflake-arctic",
        )


def test_dataframe_ai_filter_simple_text(session):
    """Test DataFrame.ai.filter with simple text predicate."""
    # Create a DataFrame with sentiment data
    df = session.create_dataframe(
        [
            ["This is amazing!"],
            ["This is terrible!"],
            ["This is okay."],
            ["I love this product!"],
            ["I hate this service."],
        ],
        schema=["review"],
    )

    # Filter for positive reviews
    positive_df = df.ai.filter(
        "Is this review positive?", input_columns=[col("review")]
    )

    # Check that we get some results (should be the positive ones)
    results = positive_df.collect(_emit_ast=False)
    assert len(results) >= 1  # At least some positive reviews should be found
    assert len(results) <= 5  # Not more than the original count

    # Verify the filtered results contain positive sentiment words
    positive_reviews = [row["REVIEW"] for row in results]
    positive_indicators = ["amazing", "love"]
    assert any(
        any(indicator in review.lower() for indicator in positive_indicators)
        for review in positive_reviews
    )


def test_dataframe_ai_filter_with_named_placeholders(session):
    """Test DataFrame.ai.filter with named placeholders."""
    # Create a DataFrame with country and continent data
    df = session.create_dataframe(
        [
            ["Switzerland", "Europe"],
            ["Korea", "Asia"],
            ["Brazil", "South America"],
            ["Germany", "Europe"],
            ["Japan", "Asia"],
        ],
        schema=["country", "continent"],
    )

    # Filter for European countries
    european_df = df.ai.filter(
        "Is {country} located in {continent} and specifically in Europe?",
        input_columns={"country": col("country"), "continent": col("continent")},
    )

    # Check results
    results = european_df.collect(_emit_ast=False)
    assert len(results) >= 1  # Should find at least one European country

    # Verify the results are European countries
    countries = [row["COUNTRY"] for row in results]
    european_countries = ["Switzerland", "Germany"]
    assert any(country in european_countries for country in countries)


def test_dataframe_ai_filter_with_positional_placeholders(session):
    """Test DataFrame.ai.filter with positional placeholders."""
    # Create a DataFrame with programming languages and their types
    df = session.create_dataframe(
        [
            ["Python", "programming"],
            ["SQL", "database"],
            ["HTML", "markup"],
            ["JavaScript", "programming"],
            ["CSS", "styling"],
        ],
        schema=["language", "type"],
    )

    # Filter for programming languages
    programming_df = df.ai.filter(
        "Is {0} primarily used for {1} and is it a programming language?",
        input_columns=[col("language"), col("type")],
    )

    # Check results
    results = programming_df.collect(_emit_ast=False)
    assert len(results) >= 1  # Should find at least one programming language

    # Verify the results contain programming languages
    languages = [row["LANGUAGE"] for row in results]
    programming_languages = ["Python", "JavaScript"]
    assert any(lang in programming_languages for lang in languages)


def test_dataframe_ai_filter_error_handling(session):
    """Test error handling in DataFrame.ai.filter."""
    df = session.create_dataframe([["test"]], schema=["text"])

    # Test invalid input_columns type
    with pytest.raises(
        TypeError, match="input_columns must be a list of Columns or a dict"
    ):
        df.ai.filter(
            "Test predicate",
            input_columns="invalid",  # Should be list or dict
        )
