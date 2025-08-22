#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import json
import pytest
from snowflake.snowpark.functions import col, lit
from snowflake.snowpark.exceptions import SnowparkSQLException


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


def test_dataframe_ai_agg_basic(session):
    """Test DataFrame.ai.agg with basic usage."""
    # Create a DataFrame with review data
    df = session.create_dataframe(
        [
            ["Excellent product, highly recommend!"],
            ["Great quality and fast shipping"],
            ["Average product, nothing special"],
            ["Poor quality, very disappointed"],
            ["Amazing value for money"],
        ],
        schema=["review"],
    )

    summary_df = df.ai.agg(
        task_description="Summarize these product reviews for a blog post targeting consumers",
        input_column=col("review"),
        output_column="summary",
    )

    # Verify results
    results = summary_df.collect(_emit_ast=False)
    assert len(results) == 1  # Should be a single aggregated row
    assert results[0]["SUMMARY"] is not None
    assert len(results[0]["SUMMARY"]) > 10  # Should have meaningful content

    summary_df = df.ai.agg(
        task_description="Summarize these product reviews for a blog post targeting consumers",
        input_column=col("review"),
    )

    # Verify results
    results = summary_df.collect(_emit_ast=False)
    assert len(results) == 1  # Should be a single aggregated row
    assert results[0]["AI_AGG_OUTPUT"] is not None
    assert len(results[0]["AI_AGG_OUTPUT"]) > 10  # Should have meaningful content


def test_dataframe_ai_agg_error_handling(session):
    """Test error handling in DataFrame.ai.agg."""
    df = session.create_dataframe([["test"]], schema=["text"])

    # Test invalid input_column type
    with pytest.raises(TypeError, match="expected Column or str"):
        df.ai.agg(
            task_description="Summarize the text",
            input_column=123,  # Invalid type
        )

    # Test invalid column name
    with pytest.raises(SnowparkSQLException, match="invalid identifier 'INVALID'"):
        df.ai.agg(
            task_description="Summarize the text",
            input_column=col("invalid"),  # Invalid column name
        ).collect(_emit_ast=False)


def test_grouped_dataframe_ai_agg(session):
    """Comprehensive test for GroupedDataFrame.ai_agg with various grouping scenarios and chained operations."""

    # Create a single DataFrame with product reviews that can be grouped in multiple ways
    df = session.create_dataframe(
        [
            ["electronics", "high", 4.5, "Excellent product, highly recommend!"],
            ["electronics", "high", 4.8, "Outstanding quality and performance"],
            ["electronics", "low", 2.5, "Not worth the price, disappointed"],
            ["electronics", "low", 2.2, "Broke after a week, poor quality"],
            ["clothing", "high", 4.2, "Perfect fit and great material"],
            ["clothing", "high", 4.6, "Beautiful design, love it!"],
            ["clothing", "low", 2.8, "Poor quality fabric, not as described"],
            ["clothing", "low", 2.1, "Color faded quickly, sizing issues"],
            ["books", "high", 4.9, "Fantastic read, couldn't put it down!"],
            ["books", "high", 4.7, "Well written and engaging"],
            ["toys", "low", 1.5, "Poor quality, broke quickly"],
            ["toys", "low", 1.8, "Not safe for children, avoid"],
        ],
        schema=["category", "quality_level", "rating", "review"],
    )

    # Test 1: Group by empty list (aggregate entire DataFrame)
    global_summary_df = df.group_by().ai_agg(
        "review",
        task_description="Create an overall summary of all customer reviews",
    )

    count = global_summary_df.count(_emit_ast=False)
    assert count == 1  # Single row for global aggregation

    # Test 2: Group by single column with string expr
    category_summary_df = (
        df.group_by("category")
        .ai_agg(
            "review",
            task_description="Summarize product reviews for a blog post",
        )
        .filter(col("CATEGORY") != "toys")  # Chain filter operation
        .sort(col("CATEGORY").asc())  # Chain sort operation
    )

    category_results = category_summary_df.collect(_emit_ast=False)
    assert len(category_results) == 3  # 3 categories after filtering out toys
    categories = [row["CATEGORY"] for row in category_results]
    assert categories == ["books", "clothing", "electronics"]  # Alphabetically sorted

    # Verify each category has a summary
    for row in category_results:
        summary_cols = [c for c in row.as_dict().keys() if c != "CATEGORY"]
        assert len(summary_cols) == 1
        assert row[summary_cols[0]] is not None
        assert len(row[summary_cols[0]]) > 10

    # Test 3: Group by single column with Column object and select operation
    quality_summary_df = (
        df.group_by("quality_level")
        .ai_agg(
            col("review"),  # Using Column object
            task_description="Extract key insights from customer feedback",
        )
        .select("QUALITY_LEVEL")  # Chain select to keep only grouping column
    )

    quality_results = quality_summary_df.collect(_emit_ast=False)
    assert len(quality_results) == 2  # Two quality levels: high and low
    assert all(
        len(row.as_dict()) == 1 for row in quality_results
    )  # Only QUALITY_LEVEL column
    quality_levels = {row["QUALITY_LEVEL"] for row in quality_results}
    assert quality_levels == {"high", "low"}

    # Test 4: Group by multiple columns with filtering and limit
    multi_group_df = (
        df.group_by(["category", "quality_level"])
        .ai_agg(
            col("review"),
            task_description="Summarize reviews highlighting common themes",
        )
        .filter(col("QUALITY_LEVEL") == "high")  # Only high quality items
        .sort(col("CATEGORY").desc())  # Sort by category descending
        .limit(2)  # Limit to top 2 results
    )

    multi_results = multi_group_df.collect(_emit_ast=False)
    assert len(multi_results) == 2  # Limited to 2 results

    # Verify the results are high quality and sorted correctly
    for row in multi_results:
        assert row["QUALITY_LEVEL"] == "high"

    # First result should be from toys or electronics (descending order)
    first_category = multi_results[0]["CATEGORY"]
    assert first_category in ["toys", "electronics", "clothing", "books"]

    # Test 5: Complex chaining with rename and additional filter
    complex_chain_df = (
        df.filter(col("rating") > 2.0)  # Pre-filter low ratings
        .group_by("category")
        .ai_agg(
            "review",
            task_description="Brief summary of positive reviews",
        )
        .filter(
            col("CATEGORY").isin(["electronics", "books"])
        )  # Keep only specific categories
        .with_column_renamed("CATEGORY", "PRODUCT_TYPE")  # Rename column
        .sort("PRODUCT_TYPE")
    )

    complex_results = complex_chain_df.collect(_emit_ast=False)
    assert len(complex_results) == 2  # Only electronics and books
    assert "PRODUCT_TYPE" in complex_results[0].as_dict()
    assert "CATEGORY" not in complex_results[0].as_dict()

    product_types = [row["PRODUCT_TYPE"] for row in complex_results]
    assert product_types == ["books", "electronics"]  # Sorted alphabetically


def test_dataframe_ai_classify_basic(session):
    """Test DataFrame.ai.classify with basic usage."""
    # Create a DataFrame with text data
    df = session.create_dataframe(
        [
            ["I love hiking in the mountains"],
            ["My favorite dish is pasta carbonara"],
            ["Just finished reading a great book"],
            ["Learning to cook Italian cuisine"],
        ],
        schema=["text"],
    )

    # Use DataFrame.ai.classify with list of categories
    result_df = df.ai.classify(
        input_column="text",
        categories=["hiking", "cooking", "reading"],
        output_column="category",
    )

    # Check schema
    assert result_df.columns == ["TEXT", "CATEGORY"]

    # Collect and verify results
    results = result_df.collect(_emit_ast=False)
    assert len(results) == 4

    # Verify some expected classifications
    text_to_category = {
        row["TEXT"]: json.loads(row["CATEGORY"])["labels"][0] for row in results
    }

    # These should be fairly obvious classifications
    assert text_to_category["My favorite dish is pasta carbonara"] == "cooking"
    assert text_to_category["Just finished reading a great book"] == "reading"
    assert text_to_category["I love hiking in the mountains"] == "hiking"


def test_dataframe_ai_classify_multi_label(session):
    """Test DataFrame.ai.classify with multi-label classification."""
    # Create a DataFrame with text that may belong to multiple categories
    df = session.create_dataframe(
        [
            ["I enjoy traveling and trying local cuisines"],
            ["Reading books while on a flight to Paris"],
            ["Cooking recipes from different countries I've visited"],
            ["Training for a marathon while exploring new cities"],
        ],
        schema=["text"],
    )
    df = df.with_column(
        "categories", lit(["travel", "cooking", "reading", "sports", "education"])
    )

    # Use multi-label classification with task description
    result_df = df.ai.classify(
        input_column=col("text"),
        categories=col("categories"),
        output_column="topics",
        task_description="Identify all topics mentioned in the text",
        output_mode="multi",
    )

    # Check schema
    assert result_df.columns == ["TEXT", "CATEGORIES", "TOPICS"]

    # Collect and verify results
    results = result_df.collect(_emit_ast=False)
    assert len(results) == 4

    # First text mentions both travel and cooking
    first_labels = json.loads(results[0]["TOPICS"])["labels"]
    assert "travel" in first_labels or "cooking" in first_labels
    # At least one row should have multiple labels
    assert any(len(json.loads(row["TOPICS"])["labels"]) > 1 for row in results)


def test_dataframe_ai_classify_with_examples(session):
    """Test DataFrame.ai.classify with few-shot examples."""
    # Create a DataFrame with ambiguous text
    df = session.create_dataframe(
        [
            ["The service was outstanding"],
            ["The product broke after one day"],
            ["Average experience, nothing special"],
            ["Exceeded all my expectations"],
        ],
        schema=["feedback"],
    )

    # Use classification with examples for better accuracy
    result_df = df.ai.classify(
        input_column="feedback",
        categories=["positive", "negative", "neutral"],
        output_column="sentiment",
        task_description="Classify customer feedback sentiment",
        examples=[
            {
                "input": "This is the best product ever",
                "labels": ["positive"],
                "explanation": "The feedback expresses strong satisfaction",
            },
            {
                "input": "Terrible quality, want my money back",
                "labels": ["negative"],
                "explanation": "The feedback expresses dissatisfaction and complaint",
            },
            {
                "input": "It is okay, not great but not bad",
                "labels": ["neutral"],
                "explanation": "The feedback shows neither strong positive nor negative sentiment",
            },
        ],
    )

    # Check schema
    assert result_df.columns == ["FEEDBACK", "SENTIMENT"]

    # Collect and verify results
    results = result_df.collect(_emit_ast=False)
    assert len(results) == 4

    # Check sentiment classifications
    feedback_to_sentiment = {
        row["FEEDBACK"]: json.loads(row["SENTIMENT"])["labels"][0] for row in results
    }

    assert feedback_to_sentiment["The service was outstanding"] == "positive"
    assert feedback_to_sentiment["The product broke after one day"] == "negative"
    assert feedback_to_sentiment["Average experience, nothing special"] == "neutral"
    assert feedback_to_sentiment["Exceeded all my expectations"] == "positive"


def test_dataframe_ai_classify_default_output_column(session):
    """Test DataFrame.ai.classify with default output column name."""
    df = session.create_dataframe([["apple"], ["carrot"], ["chicken"]], schema=["item"])

    # Don't specify output_column, should use default
    result_df = df.ai.classify(
        input_column="item",
        categories=["fruit", "vegetable", "meat", "dairy"],
    )

    # Check that default column name is used
    assert "AI_CLASSIFY_OUTPUT" in result_df.columns


def test_dataframe_ai_similarity_basic(session):
    """Test DataFrame.ai.similarity with basic text similarity."""
    # Create a DataFrame with text pairs
    df = session.create_dataframe(
        [
            ["I love programming", "I enjoy coding"],
            ["The weather is nice today", "It's a beautiful day"],
            ["Python is great", "Python is awesome"],
            ["Cats are cute", "Dogs are loyal"],
        ],
        schema=["text1", "text2"],
    )

    # Use DataFrame.ai.similarity with string column names
    result_df = df.ai.similarity(
        input1="text1",
        input2="text2",
        output_column="similarity_score",
    )

    # Check schema
    assert result_df.columns == ["TEXT1", "TEXT2", "SIMILARITY_SCORE"]

    # Collect and verify results
    results = result_df.collect(_emit_ast=False)
    assert len(results) == 4

    # Verify similarity scores are in range
    for row in results:
        score = row["SIMILARITY_SCORE"]
        assert score is not None
        assert -1 <= score <= 1

    # Verify relative similarities make sense
    # "I love programming" and "I enjoy coding" should be more similar
    # than "Cats are cute" and "Dogs are loyal"
    programming_similarity = results[0]["SIMILARITY_SCORE"]
    python_similarity = results[2]["SIMILARITY_SCORE"]
    pets_similarity = results[3]["SIMILARITY_SCORE"]

    # Python statements should be very similar
    assert python_similarity > 0.7
    # Programming statements should be somewhat similar
    assert programming_similarity > 0.4
    # Pets statements are less similar (different animals)
    assert pets_similarity < programming_similarity


def test_dataframe_ai_similarity_default_output_column(session):
    """Test DataFrame.ai.similarity with default output column name."""
    df = session.create_dataframe(
        [["apple", "orange"], ["car", "vehicle"]], schema=["word1", "word2"]
    )

    # Don't specify output_column, should use default
    result_df = df.ai.similarity(
        input1="word1",
        input2="word2",
    )

    # Check that default column name is used
    assert "AI_SIMILARITY_OUTPUT" in result_df.columns

    results = result_df.collect(_emit_ast=False)
    assert len(results) == 2
    for row in results:
        assert row["AI_SIMILARITY_OUTPUT"] is not None


def test_dataframe_ai_similarity_with_custom_model(session):
    """Test DataFrame.ai.similarity with custom embedding model."""
    # Create a DataFrame with multilingual text
    df = session.create_dataframe(
        [
            ["Hello world", "Hola mundo"],  # English and Spanish
            ["Good morning", "Guten Morgen"],  # English and German
            ["Thank you", "Merci"],  # English and French
        ],
        schema=["english", "other_language"],
    )

    # Use multilingual model for better cross-lingual similarity
    result_df = df.ai.similarity(
        input1=col("english"),
        input2=col("other_language"),
        output_column="cross_lingual_similarity",
        model="multilingual-e5-large",
    )

    # Check schema
    assert result_df.columns == [
        "ENGLISH",
        "OTHER_LANGUAGE",
        "CROSS_LINGUAL_SIMILARITY",
    ]

    # Collect and verify results
    results = result_df.collect(_emit_ast=False)
    assert len(results) == 3

    # With multilingual model, translations should have high similarity
    for row in results:
        # Translations should have moderate to high similarity with multilingual model
        assert row["CROSS_LINGUAL_SIMILARITY"] > 0.3


def test_dataframe_ai_similarity_error_handling(session):
    """Test error handling in DataFrame.ai.similarity."""
    df = session.create_dataframe([["test"]], schema=["text"])

    # Test invalid input type
    with pytest.raises(TypeError, match="expected Column or str"):
        df.ai.similarity(
            input1=123,  # Invalid type
            input2="text",
        )

    with pytest.raises(TypeError, match="expected Column or str"):
        df.ai.similarity(
            input1="text",
            input2=[col("text")],  # Invalid type (list)
        )

    # Test invalid column name
    with pytest.raises(SnowparkSQLException, match="invalid identifier"):
        df.ai.similarity(
            input1="text",
            input2="nonexistent_column",
        ).collect(_emit_ast=False)


def test_dataframe_ai_similarity_with_nulls(session):
    """Test DataFrame.ai.similarity behavior with NULL values."""
    # Create a DataFrame with some NULL values
    df = session.create_dataframe(
        [
            ["Hello", "World"],
            [None, "Test"],
            ["Test", None],
            ["Snowflake", "Database"],
        ],
        schema=["col1", "col2"],
    )

    result_df = df.ai.similarity(
        input1="col1",
        input2="col2",
        output_column="similarity",
    )

    results = result_df.collect(_emit_ast=False)
    assert len(results) == 4

    # Rows with NULLs should return NULL similarity scores
    assert results[1]["SIMILARITY"] is None  # First column is NULL
    assert results[2]["SIMILARITY"] is None  # Second column is NULL

    # Rows without NULLs should have valid scores
    assert results[0]["SIMILARITY"] is not None
    assert results[3]["SIMILARITY"] is not None
