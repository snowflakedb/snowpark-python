#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from snowflake.snowpark._internal.utils import (
    create_prompt_column_from_template,
    experimental,
)
from snowflake.snowpark.column import Column, _to_col_if_str
from snowflake.snowpark.functions import ai_complete, ai_filter, ai_agg, ai_classify
from snowflake.snowpark._internal.telemetry import add_api_call

if TYPE_CHECKING:
    import snowflake.snowpark


class DataFrameAIFunctions:
    """Provides AI-powered functions for a :class:`DataFrame`."""

    def __init__(self, dataframe: "snowflake.snowpark.DataFrame") -> None:
        self._dataframe = dataframe

    @experimental(version="1.37.0")
    def complete(
        self,
        prompt: str,
        input_columns: Union[List[Column], Dict[str, Column]],
        *,
        output_column: Optional[str] = None,
        model: Optional[str] = None,
        model_parameters: Optional[Dict[str, Any]] = None,
        _emit_ast: bool = True,
    ) -> "snowflake.snowpark.DataFrame":
        """Generate a response (completion) on each row using the specified language model.

        Args:
            prompt: The prompt template string. Use placeholders like ``{name}`` when passing a dict of columns,
                or ``{0}``, ``{1}`` when passing a list.
            input_columns: A list of Columns (positional placeholders ``{0}``, ``{1}``, ...)
                or a dict mapping placeholder names to Columns.
            output_column: The name of the output column to be appended.
                If not provided, a column named ``AI_COMPLETE_OUTPUT`` is appended.
            model: Model name to pass to the underlying function.
                It must be specified.
            model_parameters: Optional dict containing model hyperparameters:

                - temperature: Value from 0 to 1 controlling randomness (default: 0)
                - top_p: Value from 0 to 1 controlling diversity (default: 0)
                - max_tokens: Maximum number of output tokens (default: 4096, max: 8192)
                - guardrails: Enable Cortex Guard filtering (default: False)

        Returns:
            A new DataFrame with appended output columns at the end.

        Examples::

            >>> # Single column output with named placeholder
            >>> from snowflake.snowpark.functions import col
            >>> df = session.create_dataframe(
            ...     [["What is machine learning?"], ["Explain quantum computing"]],
            ...     schema=["question"]
            ... )
            >>> result_df = df.ai.complete(
            ...     prompt="Answer this question briefly: {q}",
            ...     input_columns={"q": col("question")},
            ...     output_column="answer",
            ...     model="snowflake-arctic"
            ... )
            >>> result_df.columns
            ['QUESTION', 'ANSWER']
            >>> result_df.count()
            2

            >>> #  Processing images with file input
            >>> from snowflake.snowpark.functions import to_file
            >>> # Upload images to a stage first
            >>> _ = session.sql("CREATE OR REPLACE TEMP STAGE mystage ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')").collect()
            >>> _ = session.file.put("tests/resources/kitchen.png", "@mystage", auto_compress=False)
            >>> _ = session.file.put("tests/resources/dog.jpg", "@mystage", auto_compress=False)
            >>> # Create DataFrame with image paths and questions
            >>> df = session.create_dataframe(
            ...     [
            ...         ["@mystage/kitchen.png", "What appliances are visible in this image?"],
            ...         ["@mystage/dog.jpg", "What animal is in this image?"]
            ...     ],
            ...     schema=["image_path", "question"]
            ... )
            >>> # Use ai.complete with image files
            >>> result_df = df.ai.complete(
            ...     prompt="Image: {0}, Question: {1}",
            ...     input_columns=[
            ...         to_file(col("image_path")),
            ...         col("question")
            ...     ],
            ...     output_column="answer",
            ...     model="claude-4-sonnet"
            ... )
            >>> result_df.columns
            ['IMAGE_PATH', 'QUESTION', 'ANSWER']
            >>> result_df.count()
            2
            >>> results = result_df.collect()
            >>> 'microwave' in results[0]["ANSWER"].lower()
            True
            >>> 'dog' in results[1]["ANSWER"].lower()
            True
        """

        if not model:
            raise ValueError("model must be specified for ai.complete")

        # Build the prompt Column
        if isinstance(input_columns, (dict, list)):
            prompt_obj = create_prompt_column_from_template(
                prompt, input_columns, _emit_ast=False
            )
        else:
            raise TypeError(
                "input_columns must be a list of Columns or a dict mapping placeholder names to Columns"
            )

        # Call the ai_complete function with all explicit parameters
        result_col = ai_complete(
            model=model,
            prompt=prompt_obj,
            model_parameters=model_parameters,
            _emit_ast=False,
        )

        # Add the output column to the DataFrame
        output_column_name = output_column or "AI_COMPLETE_OUTPUT"
        df = self._dataframe.with_column(
            output_column_name, result_col, _emit_ast=False
        )

        add_api_call(
            df,
            "DataFrame.ai.complete",
        )
        return df

    @experimental(version="1.37.0")
    def filter(
        self,
        predicate: str,
        input_columns: Union[List[Column], Dict[str, Column]],
        *,
        _emit_ast: bool = True,
    ) -> "snowflake.snowpark.DataFrame":
        """Filter rows using AI-powered boolean classification.

        This method applies AI-based filtering to each row, classifying them as True or False
        based on the provided predicate. Supports both text-based filtering and image filtering.

        Args:
            predicate: The classification instruction string. Use placeholders like ``{name}`` when passing
                a dict of columns, or ``{0}``, ``{1}`` when passing a list. For file-based filtering,
                this should contain instructions to classify the file as TRUE or FALSE.
            input_columns: Optional list of Columns (positional placeholders ``{0}``, ``{1}``, ...)
                or a dict mapping placeholder names to Columns. Used when predicate contains placeholders.

        Examples::

            >>> # Simple text filtering without placeholders
            >>> df = session.create_dataframe(
            ...     [["This is great!"], ["This is terrible!"], ["This is okay."]],
            ...     schema=["review"]
            ... )
            >>> positive_df = df.ai.filter("Is this review positive?", input_columns=[df["review"]])
            >>> positive_df.count()  # Should be 1 (only "This is great!")
            1

            >>> # Text filtering with named placeholders
            >>> df = session.create_dataframe(
            ...     [["Switzerland", "Europe"], ["Korea", "Asia"], ["Brazil", "South America"]],
            ...     schema=["country", "continent"]
            ... )
            >>> european_df = df.ai.filter(
            ...     "Is {country} located in {continent} and specifically in Europe?",
            ...     input_columns={"country": df["country"], "continent": df["continent"]}
            ... )
            >>> european_df.collect()[0]["COUNTRY"]
            'Switzerland'

            >>> # Image filtering with positional placeholders
            >>> from snowflake.snowpark.functions import to_file
            >>> # Upload images to a stage first
            >>> _ = session.sql("CREATE OR REPLACE TEMP STAGE mystage ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')").collect()
            >>> _ = session.file.put("tests/resources/dog.jpg", "@mystage", auto_compress=False)
            >>> _ = session.file.put("tests/resources/cat.jpeg", "@mystage", auto_compress=False)
            >>> df = session.read.file("@mystage")
            >>> dog_images_df = df.ai.filter(
            ...     "Does this image contain a dog?",
            ...     input_columns=[df["FILE"]]
            ... )
            >>> dog_images_df.count()  # Should be 1 (only dog image)
            1
        """

        # Build the predicate Column
        if isinstance(input_columns, (dict, list)):
            predicate_col = create_prompt_column_from_template(
                predicate, input_columns, _emit_ast=False
            )
        else:
            raise TypeError(
                "input_columns must be a list of Columns or a dict mapping placeholder names to Columns"
            )

        # Filter the DataFrame to only include rows where the result is True
        filter_result = ai_filter(
            predicate=predicate_col,
            _emit_ast=False,
        )
        filtered_df = self._dataframe.filter(filter_result, _emit_ast=False)

        add_api_call(
            filtered_df,
            "DataFrame.ai.filter",
        )
        return filtered_df

    @experimental(version="1.37.0")
    def agg(
        self,
        task_description: str,
        input_column: Union[str, Column],
        *,
        output_column: Optional[str] = None,
        _emit_ast: bool = True,
    ) -> "snowflake.snowpark.DataFrame":
        """Aggregate a column of text data using a natural language task description.

        This method reduces a column of text by performing a natural language aggregation
        as described in the task description. For instance, it can summarize large datasets or
        extract specific insights.

        Args:
            task_description: A plain English string that describes the aggregation task, such as
                "Summarize the product reviews for a blog post targeting consumers" or
                "Identify the most positive review and translate it into French and Polish, one word only".
            input_column: The column (Column object or column name as string) containing the text data
                on which the aggregation operation is to be performed.
            output_column: The name of the output column to be appended.
                If not provided, a column named ``AI_AGG_OUTPUT`` is appended.

        Examples::

            >>> # Aggregate product reviews
            >>> df = session.create_dataframe([
            ...     ["Excellent product, highly recommend!"],
            ...     ["Great quality and fast shipping"],
            ...     ["Average product, nothing special"],
            ...     ["Poor quality, very disappointed"],
            ... ], schema=["review"])
            >>> summary_df = df.ai.agg(
            ...     task_description="Summarize these product reviews for a blog post targeting consumers",
            ...     input_column="review",
            ...     output_column="summary"
            ... )
            >>> summary_df.columns
            ['SUMMARY']
            >>> summary_df.count()
            1

            >>> # Aggregate with Column object
            >>> from snowflake.snowpark.functions import col
            >>> df = session.create_dataframe([
            ...     ["Customer service was excellent"],
            ...     ["Product arrived damaged"],
            ...     ["Great value for money"],
            ...     ["Would buy again"],
            ... ], schema=["feedback"])
            >>> insights_df = df.ai.agg(
            ...     task_description="Extract the main positive and negative points from customer feedback",
            ...     input_column=col("feedback"),
            ...     output_column="insights"
            ... )
            >>> insights_df.count()
            1

        Note:
            For optimal performance, follow these guidelines:

                - Use plain English text for the task description.

                - Describe the text provided in the task description. For example, instead of a task
                  description like "summarize", use "Summarize the phone call transcripts".

                - Describe the intended use case. For example, instead of "find the best review",
                  use "Find the most positive and well-written restaurant review to highlight on
                  the restaurant website".

                - Consider breaking the task description into multiple steps. For example, instead of
                  "Summarize the new articles", use "You will be provided with news articles from
                  various publishers presenting events from different points of view. Please create
                  a concise and elaborative summary of source texts without missing any crucial information.".
        """

        # Call the ai_agg function
        input_col = _to_col_if_str(input_column, "DataFrame.ai.agg")
        result_col = ai_agg(
            input_col,
            task_description=task_description,
            _emit_ast=False,
        )

        # Create a new DataFrame with the aggregated result
        output_column_name = output_column or "AI_AGG_OUTPUT"
        df = self._dataframe.select(
            result_col.alias(output_column_name), _emit_ast=False
        )

        add_api_call(
            df,
            "DataFrame.ai.agg",
        )
        return df

    @experimental(version="1.37.0")
    def classify(
        self,
        input_column: Union[str, Column],
        categories: Union[List[str], Column],
        *,
        output_column: Optional[str] = None,
        _emit_ast: bool = True,
        **kwargs,
    ) -> "snowflake.snowpark.DataFrame":
        """Classify text or images into specified categories using AI.

        This method applies AI-based classification to each row, assigning one or more categories
        from the provided list based on the input content.

        Args:
            input_column: The column (Column object or column name as string) containing the text
                or image data to classify.
            categories: List of category strings or a Column containing an array of categories.
                Must contain at least 2 and no more than 100 categories.
            output_column: The name of the output column to be appended.
                If not provided, a column named ``AI_CLASSIFY_OUTPUT`` is appended.
            **kwargs: Configuration settings specified as key/value pairs. Supported keys:

                - task_description: A explanation of the classification task that is 50 words or fewer.
                  This can help the model understand the context of the classification task and improve accuracy.

                - output_mode: Set to ``multi`` for multi-label classification. Defaults to ``single`` for single-label classification.

                - examples: A list of example objects for few-shot learning. Each example must include:

                    - input: Example text to classify.
                    - labels: List of correct categories for the input.
                    - explanation: Explanation of why the input maps to those categories.

        Returns:
            A new DataFrame with an appended output column containing classification results.
            The output is a JSON object with a ``labels`` field containing the assigned categories.

        Examples::

            >>> # Simple text classification with list of categories
            >>> from snowflake.snowpark.functions import col
            >>> import json
            >>> df = session.create_dataframe(
            ...     [
            ...         ["I love hiking in the mountains"],
            ...         ["My favorite dish is pasta carbonara"],
            ...         ["Just finished reading a great book"],
            ...     ],
            ...     schema=["text"]
            ... )
            >>> result_df = df.ai.classify(
            ...     input_column="text",
            ...     categories=["hiking", "cooking", "reading"],
            ...     output_column="category"
            ... )
            >>> result_df.columns
            ['TEXT', 'CATEGORY']
            >>> results = result_df.collect()
            >>> json.loads(results[0]["CATEGORY"])["labels"][0]
            'hiking'

            >>> # Image classification with Column containing categories
            >>> from snowflake.snowpark.functions import to_file
            >>> # Upload images to a stage first
            >>> _ = session.sql("CREATE OR REPLACE TEMP STAGE mystage ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')").collect()
            >>> _ = session.file.put("tests/resources/dog.jpg", "@mystage", auto_compress=False)
            >>> _ = session.file.put("tests/resources/cat.jpeg", "@mystage", auto_compress=False)
            >>> _ = session.file.put("tests/resources/kitchen.png", "@mystage", auto_compress=False)
            >>> # Create DataFrame with image paths and possible categories for each image
            >>> df = session.create_dataframe(
            ...     [
            ...         ["@mystage/dog.jpg", ["cat", "dog", "bird", "fish"]],
            ...         ["@mystage/cat.jpeg", ["cat", "dog", "rabbit", "hamster"]],
            ...         ["@mystage/kitchen.png", ["kitchen", "bedroom", "bathroom", "living room"]],
            ...     ],
            ...     schema=["image_path", "categories"]
            ... )
            >>> # Classify images using their respective category options
            >>> result_df = df.ai.classify(
            ...     input_column=to_file(col("image_path")),
            ...     categories=col("categories"),
            ...     output_column="classification"
            ... )
            >>> result_df.columns
            ['IMAGE_PATH', 'CATEGORIES', 'CLASSIFICATION']
            >>> results = result_df.collect()
            >>> # Verify the dog image is classified as 'dog'
            >>> dog_result = [r for r in results if 'dog.jpg' in r["IMAGE_PATH"]][0]
            >>> json.loads(dog_result["CLASSIFICATION"])["labels"][0]
            'dog'

            >>> # Multi-label classification with advanced configuration
            >>> df = session.create_dataframe(
            ...     [
            ...         ["I enjoy traveling and trying local cuisines"],
            ...         ["Reading books while on a flight"],
            ...         ["Cooking recipes from different countries"],
            ...     ],
            ...     schema=["text"]
            ... )
            >>> result_df = df.ai.classify(
            ...     input_column="text",
            ...     categories=["travel", "cooking", "reading", "sports"],
            ...     output_column="topics",
            ...     task_description="Identify all topics mentioned in the text",
            ...     output_mode="multi",
            ...     examples=[{
            ...         "input": "I love reading cookbooks during my travels",
            ...         "labels": ["travel", "cooking", "reading"],
            ...         "explanation": "The text mentions traveling, cookbooks (cooking), and reading"
            ...     }]
            ... )
            >>> result_df.columns
            ['TEXT', 'TOPICS']
            >>> results = result_df.collect()
            >>> len(json.loads(results[0]["TOPICS"])["labels"]) >= 1  # Multi-label can have multiple labels
            True
        """

        # Convert string input column to Column object
        input_col = _to_col_if_str(input_column, "DataFrame.ai.classify")

        # Call the ai_classify function
        result_col = ai_classify(
            input_col,
            categories,
            _emit_ast=False,
            **kwargs,
        )

        # Add the output column to the DataFrame
        output_column_name = output_column or "AI_CLASSIFY_OUTPUT"
        df = self._dataframe.with_column(
            output_column_name, result_col, _emit_ast=False
        )

        add_api_call(
            df,
            "DataFrame.ai.classify",
        )
        return df
