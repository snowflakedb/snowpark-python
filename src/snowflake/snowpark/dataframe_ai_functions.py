#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Union

from snowflake.snowpark._internal.utils import (
    create_prompt_column_from_template,
    experimental,
    publicapi,
)
from snowflake.snowpark._internal.ast.utils import (
    build_expr_from_python_val,
    build_expr_from_snowpark_column_or_col_name,
    build_expr_from_snowpark_column_or_python_val,
    with_src_position,
)
from snowflake.snowpark._internal.type_utils import ColumnOrName
from snowflake.snowpark.column import Column, _to_col_if_str, _to_col_if_lit
from snowflake.snowpark.functions import (
    ai_complete,
    ai_filter,
    ai_agg,
    ai_classify,
    ai_extract,
    ai_similarity,
    ai_sentiment,
    ai_embed,
    ai_summarize_agg,
    ai_transcribe,
    ai_parse_document,
    function,
)
from snowflake.snowpark._internal.telemetry import add_api_call

if TYPE_CHECKING:
    import snowflake.snowpark


class DataFrameAIFunctions:
    """Provides AI-powered functions for a :class:`DataFrame`."""

    def __init__(self, dataframe: "snowflake.snowpark.DataFrame") -> None:
        self._dataframe = dataframe

    @experimental(version="1.39.0")
    @publicapi
    def complete(
        self,
        prompt: str,
        input_columns: Union[List[Column], Dict[str, Column]],
        model: str,
        *,
        output_column: Optional[str] = None,
        model_parameters: Optional[Dict[str, Any]] = None,
        _emit_ast: bool = True,
    ) -> "snowflake.snowpark.DataFrame":
        """Generate a response (completion) on each row using the specified language model.

        Args:
            prompt: The prompt template string. Use placeholders like ``{name}`` when passing a dict of columns,
                or ``{0}``, ``{1}`` when passing a list.
            input_columns: A list of Columns (positional placeholders ``{0}``, ``{1}``, ...)
                or a dict mapping placeholder names to Columns.
            model: A string specifying the model to be used. Different input types have different supported models.
                See details in `AI_COMPLETE <https://docs.snowflake.com/en/sql-reference/functions/ai_complete>`_.
            output_column: The name of the output column to be appended.
                If not provided, a column named ``AI_COMPLETE_OUTPUT`` is appended
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

        # Build the prompt Column
        if isinstance(input_columns, (dict, list)):
            prompt_obj = create_prompt_column_from_template(
                prompt, input_columns, _emit_ast=_emit_ast
            )
        else:
            raise TypeError(
                "input_columns must be a list of Columns or a dict mapping placeholder names to Columns"
            )

        output_column_name = output_column or "AI_COMPLETE_OUTPUT"

        # AST at top
        stmt = None
        if _emit_ast:
            stmt = self._dataframe._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_ai_complete, stmt)
            self._dataframe._set_ast_ref(ast.df)
            ast.model = model
            ast.prompt = prompt
            # populate input_columns with the prompt column expression
            build_expr_from_snowpark_column_or_col_name(ast.input_columns, prompt_obj)
            if model_parameters:
                for k, v in model_parameters.items():
                    entry = ast.model_parameters.add()
                    entry._1 = k
                    build_expr_from_python_val(entry._2, v)

            ast.output_column.value = output_column_name

        # Call the ai_complete function with all explicit parameters
        result_col = ai_complete(
            model=model,
            prompt=prompt_obj,
            model_parameters=model_parameters,
            _emit_ast=False,
        )

        # Add the output column to the DataFrame
        df = self._dataframe.with_column(
            output_column_name, result_col, _emit_ast=False
        )

        add_api_call(
            df,
            "DataFrame.ai.complete",
        )
        if _emit_ast:
            df._ast_id = stmt.uid
        return df

    @experimental(version="1.39.0")
    @publicapi
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

        # AST at top
        stmt = None
        predicate_ast_col = None
        if _emit_ast:
            if isinstance(input_columns, (dict, list)):
                predicate_ast_col = create_prompt_column_from_template(
                    predicate, input_columns, _emit_ast=True
                )
            else:
                raise TypeError(
                    "input_columns must be a list of Columns or a dict mapping placeholder names to Columns"
                )
            stmt = self._dataframe._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_ai_filter, stmt)
            self._dataframe._set_ast_ref(ast.df)
            ast.predicate = predicate
            build_expr_from_snowpark_column_or_col_name(
                ast.input_columns, predicate_ast_col
            )

        # Build the predicate Column for execution
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
        if _emit_ast:
            filtered_df._ast_id = stmt.uid
        return filtered_df

    @experimental(version="1.39.0")
    @publicapi
    def agg(
        self,
        task_description: str,
        input_column: ColumnOrName,
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
        output_column_name = output_column or "AI_AGG_OUTPUT"

        # AST at top
        stmt = None
        input_col = _to_col_if_str(input_column, "DataFrame.ai.agg")
        if _emit_ast:
            stmt = self._dataframe._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_ai_agg, stmt)
            self._dataframe._set_ast_ref(ast.df)
            build_expr_from_snowpark_column_or_col_name(ast.input_column, input_col)
            ast.task_description = task_description

            ast.output_column.value = output_column_name

        # Call the ai_agg function
        result_col = ai_agg(
            input_col,
            task_description=task_description,
            _emit_ast=False,
        )

        # Create a new DataFrame with the aggregated result
        df = self._dataframe.select(
            result_col.alias(output_column_name), _emit_ast=False
        )

        add_api_call(
            df,
            "DataFrame.ai.agg",
        )
        if _emit_ast:
            df._ast_id = stmt.uid
        return df

    @experimental(version="1.39.0")
    @publicapi
    def classify(
        self,
        input_column: ColumnOrName,
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

        output_column_name = output_column or "AI_CLASSIFY_OUTPUT"

        # Convert string input column to Column object and AST at top
        stmt = None
        input_col = _to_col_if_str(input_column, "DataFrame.ai.classify")
        if _emit_ast:
            stmt = self._dataframe._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_ai_classify, stmt)
            self._dataframe._set_ast_ref(ast.df)
            build_expr_from_snowpark_column_or_col_name(ast.input_column, input_col)
            build_expr_from_snowpark_column_or_python_val(ast.categories, categories)
            for k, v in kwargs.items():
                entry = ast.kwargs.add()
                entry._1 = k
                build_expr_from_python_val(entry._2, v)

            ast.output_column.value = output_column_name

        # Call the ai_classify function
        result_col = ai_classify(
            input_col,
            categories,
            _emit_ast=False,
            **kwargs,
        )

        # Add the output column to the DataFrame
        df = self._dataframe.with_column(
            output_column_name, result_col, _emit_ast=False
        )

        add_api_call(
            df,
            "DataFrame.ai.classify",
        )
        if _emit_ast:
            df._ast_id = stmt.uid
        return df

    @experimental(version="1.39.0")
    @publicapi
    def similarity(
        self,
        input1: ColumnOrName,
        input2: ColumnOrName,
        *,
        output_column: Optional[str] = None,
        _emit_ast: bool = True,
        **kwargs,
    ) -> "snowflake.snowpark.DataFrame":
        """Compute similarity scores between two columns using AI-powered embeddings.

        This method computes a similarity score based on the vector cosine similarity
        of the inputs' embedding vectors. Supports both text and image similarity.

        Args:
            input1: The first column (Column object or column name as string) for comparison.
                Can contain text strings or images (FILE data type).
            input2: The second column (Column object or column name as string) for comparison.
                Must be the same type as input1 (both text or both images).
            output_column: The name of the output column to be appended.
                If not provided, a column named ``AI_SIMILARITY_OUTPUT`` is appended.
            **kwargs: Configuration settings specified as key/value pairs. Supported keys:

                - model: The embedding model used for embeddings.
                  For text input, defaults to 'snowflake-arctic-embed-l-v2'.
                  For image input, defaults to 'voyage-multimodal-3'.
                  Supported models include:

                    - Text: 'snowflake-arctic-embed-l-v2', 'nv-embed-qa-4',
                      'multilingual-e5-large', 'voyage-multilingual-2',
                      'snowflake-arctic-embed-m-v1.5', 'snowflake-arctic-embed-m',
                      'e5-base-v2'
                    - Images: 'voyage-multimodal-3'

        Returns:
            A new DataFrame with an appended output column containing similarity scores.
            The scores range from -1 to 1, where higher values indicate greater similarity.

        Examples::

            >>> # Text similarity between two columns
            >>> from snowflake.snowpark.functions import col
            >>> df = session.create_dataframe(
            ...     [
            ...         ["I love programming", "I enjoy coding"],
            ...         ["The weather is nice", "It's raining heavily"],
            ...         ["Python is great", "Python is awesome"],
            ...     ],
            ...     schema=["text1", "text2"]
            ... )
            >>> result_df = df.ai.similarity(
            ...     input1="text1",
            ...     input2="text2",
            ...     output_column="similarity_score"
            ... )
            >>> result_df.columns
            ['TEXT1', 'TEXT2', 'SIMILARITY_SCORE']
            >>> results = result_df.collect()
            >>> results[0]["SIMILARITY_SCORE"] > 0.5  # Similar texts
            True

            >>> # Multilingual text similarity with custom model
            >>> df = session.create_dataframe(
            ...     [
            ...         ["I love programming", "我喜欢编程"],  # Same meaning in English and Chinese
            ...         ["Good morning", "Buenas noches"],  # Different meanings
            ...     ],
            ...     schema=["english", "other_language"]
            ... )
            >>> result_df = df.ai.similarity(
            ...     input1=col("english"),
            ...     input2=col("other_language"),
            ...     output_column="cross_lingual_similarity",
            ...     model="multilingual-e5-large"
            ... )
            >>> result_df.columns
            ['ENGLISH', 'OTHER_LANGUAGE', 'CROSS_LINGUAL_SIMILARITY']

            >>> # Image similarity
            >>> from snowflake.snowpark.functions import to_file
            >>> # Upload images to a stage first
            >>> _ = session.sql("CREATE OR REPLACE TEMP STAGE mystage ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')").collect()
            >>> _ = session.file.put("tests/resources/dog.jpg", "@mystage", auto_compress=False)
            >>> _ = session.file.put("tests/resources/cat.jpeg", "@mystage", auto_compress=False)
            >>> _ = session.file.put("tests/resources/kitchen.png", "@mystage", auto_compress=False)
            >>> # Create DataFrame with image pairs
            >>> df = session.create_dataframe(
            ...     [
            ...         ["@mystage/dog.jpg", "@mystage/cat.jpeg"],  # Animal comparison
            ...         ["@mystage/dog.jpg", "@mystage/kitchen.png"],  # Animal vs non-animal
            ...     ],
            ...     schema=["image1", "image2"]
            ... )
            >>> result_df = df.ai.similarity(
            ...     input1=to_file(col("image1")),
            ...     input2=to_file(col("image2")),
            ...     output_column="visual_similarity"
            ... )
            >>> result_df.columns
            ['IMAGE1', 'IMAGE2', 'VISUAL_SIMILARITY']
            >>> results = result_df.collect()
            >>> # Dog and cat (both animals) should be more similar than dog and kitchen
            >>> results[0]["VISUAL_SIMILARITY"] > results[1]["VISUAL_SIMILARITY"]
            True

        Note:
            - Both inputs must be of the same type (both text or both images)
            - AI_SIMILARITY does not support computing similarity between text and image inputs
            - Similarity scores range from -1 to 1, where:
                - 1 indicates identical or very similar content
                - 0 indicates no similarity
                - -1 indicates opposite or very dissimilar content
        """
        output_column_name = output_column or "AI_SIMILARITY_OUTPUT"

        # Convert string inputs to Column objects and AST at top
        stmt = None
        input1_col = _to_col_if_str(input1, "DataFrame.ai.similarity")
        input2_col = _to_col_if_str(input2, "DataFrame.ai.similarity")
        if _emit_ast:
            stmt = self._dataframe._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_ai_similarity, stmt)
            self._dataframe._set_ast_ref(ast.df)
            build_expr_from_snowpark_column_or_col_name(ast.input1, input1_col)
            build_expr_from_snowpark_column_or_col_name(ast.input2, input2_col)
            for k, v in kwargs.items():
                entry = ast.kwargs.add()
                entry._1 = k
                build_expr_from_python_val(entry._2, v)

            ast.output_column.value = output_column_name

        # Call the ai_similarity function
        result_col = ai_similarity(
            input1_col,
            input2_col,
            _emit_ast=False,
            **kwargs,
        )

        # Add the output column to the DataFrame
        df = self._dataframe.with_column(
            output_column_name, result_col, _emit_ast=False
        )

        add_api_call(
            df,
            "DataFrame.ai.similarity",
        )
        if _emit_ast:
            df._ast_id = stmt.uid
        return df

    @experimental(version="1.39.0")
    @publicapi
    def sentiment(
        self,
        input_column: ColumnOrName,
        categories: Optional[List[str]] = None,
        *,
        output_column: Optional[str] = None,
        _emit_ast: bool = True,
    ) -> "snowflake.snowpark.DataFrame":
        """Extract sentiment analysis from text content.

        This method analyzes the sentiment of text in each row, providing overall sentiment
        and optionally sentiment for specific categories or aspects mentioned in the text.

        Args:
            input_column: The column (Column object or column name as string) containing the text
                to analyze for sentiment.
            categories: Optional list of up to 10 categories (also called entities or aspects) for which
                sentiment should be extracted. Each category may be a maximum of 30 characters long.
                For example, if extracting sentiment from restaurant reviews, you might specify
                ``['cost', 'quality', 'service', 'wait time']`` as categories. If not provided,
                only overall sentiment is returned.
            output_column: The name of the output column to be appended.
                If not provided, a column named ``AI_SENTIMENT_OUTPUT`` is appended.

        Returns:
            A new DataFrame with an appended output column containing sentiment results.
            The output is a JSON object with a ``categories`` field containing an array of records.
            Each record includes:

                - ``name``: The category name (``overall`` for overall sentiment)
                - ``sentiment``: One of ``unknown``, ``positive``, ``negative``, ``neutral``, or ``mixed``

        Examples::

            >>> # Overall sentiment analysis
            >>> df = session.create_dataframe([
            ...     ["The movie had amazing visual effects but the plot was terrible."],
            ...     ["The food was delicious but the service was slow."],
            ...     ["Everything about this experience was perfect!"],
            ... ], schema=["review"])
            >>> result_df = df.ai.sentiment(
            ...     input_column="review",
            ...     output_column="sentiment"
            ... )
            >>> result_df.columns
            ['REVIEW', 'SENTIMENT']
            >>> results = result_df.collect()
            >>> import json
            >>> overall_sentiment = json.loads(results[2]["SENTIMENT"])["categories"][0]
            >>> overall_sentiment["name"]
            'overall'
            >>> overall_sentiment["sentiment"]
            'positive'

            >>> # Sentiment analysis with specific categories
            >>> from snowflake.snowpark.functions import col
            >>> df = session.create_dataframe([
            ...     ["The hotel room was spacious and clean, but the wifi was terrible and the breakfast was mediocre."],
            ...     ["Great location and friendly staff, though the parking was expensive."],
            ... ], schema=["review"])
            >>> result_df = df.ai.sentiment(
            ...     input_column=col("review"),
            ...     categories=["room", "wifi", "breakfast", "location", "staff", "parking"],
            ...     output_column="detailed_sentiment"
            ... )
            >>> result_df.columns
            ['REVIEW', 'DETAILED_SENTIMENT']
            >>> results = result_df.collect()
            >>> sentiments = json.loads(results[0]["DETAILED_SENTIMENT"])["categories"]
            >>> # Check that we have sentiments for overall plus the specified categories
            >>> len(sentiments) > 1
            True
            >>> category_names = [s["name"] for s in sentiments]
            >>> "overall" in category_names
            True
            >>> "room" in category_names
            True

        Note:
            AI_SENTIMENT can analyze sentiment in English, French, German, Hindi, Italian, Spanish,
            and Portuguese. You can specify categories in the language of the text or in English.
        """
        output_column_name = output_column or "AI_SENTIMENT_OUTPUT"

        # Convert string input column and AST at top
        stmt = None
        input_col = _to_col_if_str(input_column, "DataFrame.ai.sentiment")
        if _emit_ast:
            stmt = self._dataframe._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_ai_sentiment, stmt)
            self._dataframe._set_ast_ref(ast.df)
            build_expr_from_snowpark_column_or_col_name(ast.input_column, input_col)
            if categories is not None:
                build_expr_from_python_val(ast.categories, categories)

            ast.output_column.value = output_column_name

        # Call the ai_sentiment function
        result_col = ai_sentiment(
            input_col,
            categories=categories,
            _emit_ast=False,
        )

        # Add the output column to the DataFrame
        df = self._dataframe.with_column(
            output_column_name, result_col, _emit_ast=False
        )

        add_api_call(
            df,
            "DataFrame.ai.sentiment",
        )
        if _emit_ast:
            df._ast_id = stmt.uid
        return df

    @experimental(version="1.39.0")
    @publicapi
    def embed(
        self,
        input_column: ColumnOrName,
        model: str,
        *,
        output_column: Optional[str] = None,
        _emit_ast: bool = True,
    ) -> "snowflake.snowpark.DataFrame":
        """Generate embedding vectors from text or images.

        This method creates dense vector representations (embeddings) of text or images,
        which can be used for similarity search, clustering, or as features for machine learning.

        Args:
            input_column: The column (Column object or column name as string) containing the text
                or images (FILE data type) to embed.
            model: The embedding model to use. Supported models:

                For text embeddings:
                    - ``snowflake-arctic-embed-l-v2.0``: Arctic large model (default for text)
                    - ``snowflake-arctic-embed-l-v2.0-8k``: Arctic large model with 8K context
                    - ``nv-embed-qa-4``: NVIDIA embedding model for Q&A
                    - ``multilingual-e5-large``: Multilingual embedding model
                    - ``voyage-multilingual-2``: Voyage multilingual model

                For image embeddings:
                    - ``voyage-multimodal-3``: Voyage multimodal model (only for images)

            output_column: The name of the output column to be appended.
                If not provided, a column named ``AI_EMBED_OUTPUT`` is appended.

        Returns:
            A new DataFrame with an appended output column containing VECTOR embeddings.

        Examples::

            >>> # Text embeddings with default model
            >>> df = session.create_dataframe([
            ...     ["Machine learning is fascinating"],
            ...     ["Snowflake provides cloud data platform"],
            ...     ["Python is a versatile programming language"],
            ... ], schema=["text"])
            >>> result_df = df.ai.embed(
            ...     input_column="text",
            ...     model="snowflake-arctic-embed-l-v2.0",
            ...     output_column="text_vector"
            ... )
            >>> results = result_df.collect()
            >>> # Verify we got embeddings
            >>> all(len(row["TEXT_VECTOR"]) > 0 for row in results)
            True

            >>> # Multilingual text embeddings
            >>> from snowflake.snowpark.functions import col
            >>> df = session.create_dataframe([
            ...     ["Hello world"],
            ...     ["Bonjour le monde"],
            ...     ["Hola mundo"],
            ...     ["你好世界"],
            ... ], schema=["greeting"])
            >>> result_df = df.ai.embed(
            ...     input_column=col("greeting"),
            ...     model="multilingual-e5-large",
            ...     output_column="multilingual_vector"
            ... )
            >>> results = result_df.collect()
            >>> # All greetings should have embeddings
            >>> all(len(row["MULTILINGUAL_VECTOR"]) > 0 for row in results)
            True

            >>> # Image embeddings
            >>> from snowflake.snowpark.functions import to_file
            >>> # Upload images to a stage first
            >>> _ = session.sql("CREATE OR REPLACE TEMP STAGE mystage ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')").collect()
            >>> _ = session.file.put("tests/resources/dog.jpg", "@mystage", auto_compress=False)
            >>> _ = session.file.put("tests/resources/cat.jpeg", "@mystage", auto_compress=False)
            >>> df = session.read.file("@mystage")
            >>> result_df = df.ai.embed(
            ...     input_column="file",
            ...     model="voyage-multimodal-3",
            ...     output_column="image_vector"
            ... )
            >>> results = result_df.collect()
            >>> # Both images should have embeddings
            >>> all(len(row["IMAGE_VECTOR"]) > 0 for row in results)
            True

        Note:
            - Embeddings can be used with vector similarity functions to find similar items
            - Different models produce embeddings of different dimensions
            - For best results, use the same model for all items you want to compare
        """
        output_column_name = output_column or "AI_EMBED_OUTPUT"

        # Convert string input column to Column object & AST at top
        stmt = None
        input_col = _to_col_if_str(input_column, "DataFrame.ai.embed")
        if _emit_ast:
            stmt = self._dataframe._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_ai_embed, stmt)
            self._dataframe._set_ast_ref(ast.df)
            build_expr_from_snowpark_column_or_col_name(ast.input_column, input_col)
            ast.model = model

            ast.output_column.value = output_column_name

        # Call the ai_embed function
        result_col = ai_embed(
            model=model,
            input=input_col,
            _emit_ast=False,
        )

        # Add the output column to the DataFrame
        df = self._dataframe.with_column(
            output_column_name, result_col, _emit_ast=False
        )

        add_api_call(
            df,
            "DataFrame.ai.embed",
        )
        if _emit_ast:
            df._ast_id = stmt.uid
        return df

    @experimental(version="1.39.0")
    @publicapi
    def summarize_agg(
        self,
        input_column: ColumnOrName,
        *,
        output_column: Optional[str] = None,
        _emit_ast: bool = True,
    ) -> "snowflake.snowpark.DataFrame":
        """Summarize a column of text data using AI.

        This method aggregates and summarizes text data from multiple rows into a single
        comprehensive summary. It's particularly useful for creating summaries from
        collections of reviews, feedback, transcripts, or other text content.

        Args:
            input_column: The column (Column object or column name as string) containing the text
                data to summarize.
            output_column: The name of the output column to be appended.
                If not provided, a column named ``AI_SUMMARIZE_AGG_OUTPUT`` is appended.

        Returns:
            A new DataFrame with a single row containing the summarized text.

        Examples::

            >>> # Summarize product reviews
            >>> df = session.create_dataframe([
            ...     ["The product quality is excellent and shipping was fast."],
            ...     ["Great value for money, highly recommend!"],
            ...     ["Customer service was very helpful and responsive."],
            ...     ["The packaging could be better, but the product itself is good."],
            ...     ["Easy to use and works as advertised."],
            ... ], schema=["review"])
            >>> summary_df = df.ai.summarize_agg(
            ...     input_column="review",
            ...     output_column="reviews_summary"
            ... )
            >>> summary_df.columns
            ['REVIEWS_SUMMARY']
            >>> summary_df.count()
            1
            >>> results = summary_df.collect()
            >>> len(results[0]["REVIEWS_SUMMARY"]) > 10
            True

            >>> # Summarize with Column object
            >>> from snowflake.snowpark.functions import col
            >>> df = session.create_dataframe([
            ...     ["Meeting started with project updates"],
            ...     ["Discussed timeline and deliverables"],
            ...     ["Identified key risks and mitigation strategies"],
            ...     ["Assigned action items to team members"],
            ... ], schema=["meeting_notes"])
            >>> summary_df = df.ai.summarize_agg(
            ...     input_column=col("meeting_notes"),
            ...     output_column="meeting_summary"
            ... )
            >>> summary_df.columns
            ['MEETING_SUMMARY']
            >>> summary_df.count()
            1

        Note:
            - This is an aggregation function that combines multiple rows into a single summary
            - For best results, provide clear and coherent text in the input column
            - The summary will capture the main themes and important points from all input rows
            - Unlike the ``agg`` method which requires a task description, ``summarize_agg``
              automatically generates a comprehensive summary
        """
        output_column_name = output_column or "AI_SUMMARIZE_AGG_OUTPUT"

        # Convert string input column to Column object & AST at top
        stmt = None
        input_col = _to_col_if_str(input_column, "DataFrame.ai.summarize_agg")
        if _emit_ast:
            stmt = self._dataframe._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_ai_summarize_agg, stmt)
            self._dataframe._set_ast_ref(ast.df)
            build_expr_from_snowpark_column_or_col_name(ast.input_column, input_col)

            ast.output_column.value = output_column_name

        # Call the ai_summarize_agg function
        result_col = ai_summarize_agg(
            input_col,
            _emit_ast=False,
        )

        # Create a new DataFrame with the summarized result
        df = self._dataframe.select(
            result_col.alias(output_column_name), _emit_ast=False
        )

        add_api_call(
            df,
            "DataFrame.ai.summarize_agg",
        )
        if _emit_ast:
            df._ast_id = stmt.uid
        return df

    @experimental(version="1.39.0")
    @publicapi
    def transcribe(
        self,
        input_column: ColumnOrName,
        *,
        output_column: Optional[str] = None,
        _emit_ast: bool = True,
        **kwargs,
    ) -> "snowflake.snowpark.DataFrame":
        """Transcribe text from an audio file with optional timestamps and speaker labels.

        Args:
            input_column: The column (Column object or column name as string) containing FILE references
                to audio files. Use ``to_file`` to convert staged paths to FILE type.
            output_column: The name of the output column to be appended.
                If not provided, a column named ``AI_TRANSCRIBE_OUTPUT`` is appended.
            **kwargs: Additional options forwarded to the underlying function, e.g. ``timestamp_granularity``.

        Examples::

            >>> import json
            >>> # Basic transcription without timestamps
            >>> _ = session.sql("CREATE OR REPLACE TEMP STAGE mystage ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')").collect()
            >>> _ = session.file.put("tests/resources/audio.ogg", "@mystage", auto_compress=False)
            >>> from snowflake.snowpark.functions import col, to_file
            >>> df = session.create_dataframe([["@mystage/audio.ogg"]], schema=["audio_path"])  # staged file path
            >>> result_df = df.ai.transcribe(
            ...     input_column=to_file(col("audio_path")),
            ...     output_column="transcript",
            ... )
            >>> result_df.columns
            ['AUDIO_PATH', 'TRANSCRIPT']
            >>> result = json.loads(result_df.collect()[0]["TRANSCRIPT"])
            >>> result['audio_duration'] > 120
            True
            >>> "glad to see things are going well" in result['text'].lower()
            True

            >>> # Transcription with word-level timestamps
            >>> result_df = df.ai.transcribe(
            ...     input_column=to_file(col("audio_path")),
            ...     output_column="transcript",
            ...     timestamp_granularity='word',
            ... )
            >>> result = json.loads(result_df.collect()[0]["TRANSCRIPT"])
            >>> len(result["segments"]) > 0
            True
            >>> result["segments"][0]["text"].lower()
            'glad'
            >>> 'start' in result["segments"][0] and 'end' in result["segments"][0]
            True

            >>> # Transcription with speaker diarization (requires a multi-speaker audio file)
            >>> _ = session.file.put("tests/resources/conversation.ogg", "@mystage", auto_compress=False)
            >>> df = session.create_dataframe([["@mystage/conversation.ogg"]], schema=["audio_path"])
            >>> result_df = df.ai.transcribe(
            ...     input_column=to_file(col("audio_path")),
            ...     output_column="transcript",
            ...     timestamp_granularity='speaker',
            ... )
            >>> result = json.loads(result_df.collect()[0]["TRANSCRIPT"])
            >>> result["audio_duration"] > 100 and len(result["segments"]) > 0
            True
            >>> result["segments"][0]["speaker_label"]
            'SPEAKER_00'
            >>> 'jenny' in result["segments"][0]["text"].lower()
            True
            >>> 'start' in result["segments"][0] and 'end' in result["segments"][0]
            True
        """
        output_column_name = output_column or "AI_TRANSCRIBE_OUTPUT"

        stmt = None
        input_col = _to_col_if_str(input_column, "DataFrame.ai.transcribe")
        if _emit_ast:
            stmt = self._dataframe._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_ai_transcribe, stmt)
            self._dataframe._set_ast_ref(ast.df)
            build_expr_from_snowpark_column_or_col_name(ast.input_column, input_col)
            for k, v in kwargs.items():
                entry = ast.kwargs.add()
                entry._1 = k
                build_expr_from_python_val(entry._2, v)

            ast.output_column.value = output_column_name

        result_col = ai_transcribe(
            input_col,
            _emit_ast=False,
            **kwargs,
        )

        df = self._dataframe.with_column(
            output_column_name, result_col, _emit_ast=False
        )

        add_api_call(
            df,
            "DataFrame.ai.transcribe",
        )
        if _emit_ast:
            df._ast_id = stmt.uid
        return df

    @experimental(version="1.39.0")
    @publicapi
    def parse_document(
        self,
        input_column: ColumnOrName,
        *,
        output_column: Optional[str] = None,
        _emit_ast: bool = True,
        **kwargs,
    ) -> "snowflake.snowpark.DataFrame":
        """Extract content from a document (OCR or layout parsing) as JSON text.

        Args:
            input_column: The column (Column object or column name as string) containing FILE references
                to documents or images on a stage. Use ``to_file`` to convert staged paths to FILE type.
            output_column: The name of the output column to be appended.
                If not provided, a column named ``AI_PARSE_DOCUMENT_OUTPUT`` is appended.
            **kwargs: Additional options forwarded to the underlying function, such as ``mode`` and ``page_split``.

        Examples::

            >>> import json
            >>> # Parse a PDF document with default OCR mode
            >>> _ = session.sql("CREATE OR REPLACE TEMP STAGE mystage ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')").collect()
            >>> _ = session.file.put("tests/resources/doc.pdf", "@mystage", auto_compress=False)
            >>> from snowflake.snowpark.functions import col, to_file
            >>> df = session.create_dataframe([["@mystage/doc.pdf"]], schema=["file_path"])  # staged file path
            >>> result_df = df.ai.parse_document(
            ...     input_column=to_file(col("file_path")),
            ...     output_column="parsed",
            ... )
            >>> result_df.columns
            ['FILE_PATH', 'PARSED']
            >>> result = json.loads(result_df.collect()[0]["PARSED"])
            >>> "Sample PDF" in result["content"] and result["metadata"]["pageCount"] == 3
            True

            >>> # Parse with LAYOUT mode to extract tables and structure
            >>> _ = session.file.put("tests/resources/invoice.pdf", "@mystage", auto_compress=False)
            >>> df = session.create_dataframe([["@mystage/invoice.pdf"]], schema=["file_path"])
            >>> result_df = df.ai.parse_document(
            ...     input_column=to_file(col("file_path")),
            ...     output_column="parsed",
            ...     mode='LAYOUT',
            ... )
            >>> result = json.loads(result_df.collect()[0]["PARSED"])
            >>> "| Customer Name |" in result["content"] and "| Country |" in result["content"]
            True

            >>> # Parse with page splitting for long documents (PDF only)
            >>> df = session.create_dataframe([["@mystage/doc.pdf"]], schema=["file_path"])
            >>> result_df = df.ai.parse_document(
            ...     input_column=to_file(col("file_path")),
            ...     output_column="parsed",
            ...     page_split=True,
            ... )
            >>> result = json.loads(result_df.collect()[0]["PARSED"])
            >>> len(result["pages"]) == 3 and result["pages"][0]["index"] == 0
            True
        """
        output_column_name = output_column or "AI_PARSE_DOCUMENT_OUTPUT"

        stmt = None
        input_col = _to_col_if_str(input_column, "DataFrame.ai.parse_document")
        if _emit_ast:
            stmt = self._dataframe._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_ai_parse_document, stmt)
            self._dataframe._set_ast_ref(ast.df)
            build_expr_from_snowpark_column_or_col_name(ast.input_column, input_col)
            for k, v in kwargs.items():
                entry = ast.kwargs.add()
                entry._1 = k
                build_expr_from_python_val(entry._2, v)
            ast.output_column.value = output_column_name

        result_col = ai_parse_document(
            input_col,
            _emit_ast=False,
            **kwargs,
        )

        df = self._dataframe.with_column(
            output_column_name, result_col, _emit_ast=False
        )

        add_api_call(
            df,
            "DataFrame.ai.parse_document",
        )
        if _emit_ast:
            df._ast_id = stmt.uid
        return df

    @experimental(version="1.39.0")
    @publicapi
    def extract(
        self,
        input_column: ColumnOrName,
        *,
        response_format: Optional[Union[Dict[str, str], List]] = None,
        output_column: Optional[str] = None,
        _emit_ast: bool = True,
    ) -> "snowflake.snowpark.DataFrame":
        """Extract structured information from text or files using a response schema.

        Args:
            input_column: The column (Column object or column name as string) containing the text
                or FILE data to extract information from. Use ``to_file`` for staged file paths.
            response_format: The schema describing information to extract. Supports:

                - Simple object schema (dict) mapping feature names to extraction prompts:
                  ``{'name': 'What is the last name of the employee?', 'address': 'What is the address of the employee?'}``
                - Array of strings containing the information to be extracted:
                  ``['What is the last name of the employee?', 'What is the address of the employee?']``
                - Array of arrays containing two strings (feature name and extraction prompt):
                  ``[['name', 'What is the last name of the employee?'], ['address', 'What is the address of the employee?']]``
                - Array of strings with colon-separated feature names and extraction prompts:
                  ``['name: What is the last name of the employee?', 'address: What is the address of the employee?']``

            output_column: The name of the output column to be appended.
                If not provided, a column named ``AI_EXTRACT_OUTPUT`` is appended.

        Returns:
            A new DataFrame with an appended JSON object containing the extracted fields
            under ``response``.

        Examples::

            >>> # Extract from text string
            >>> from snowflake.snowpark.functions import col
            >>> df = session.create_dataframe([
            ...     ["John Smith lives in San Francisco and works for Snowflake"],
            ... ], schema=["text"])
            >>> result_df = df.ai.extract(
            ...     input_column="text",
            ...     response_format={'name': 'What is the first name of the employee?', 'city': 'What is the address of the employee?'},
            ...     output_column="extracted",
            ... )
            >>> result_df.select("EXTRACTED").show()
            --------------------------------
            |"EXTRACTED"                   |
            --------------------------------
            |{                             |
            |  "error": null,              |
            |  "response": {               |
            |    "city": "San Francisco",  |
            |    "name": "John"            |
            |  }                           |
            |}                             |
            --------------------------------
            <BLANKLINE>

            >>> # Extract using array format
            >>> df = session.create_dataframe(
            ...     [
            ...         ["Alice Johnson works in Seattle"],
            ...         ["Bob Williams works in Portland"],
            ...     ],
            ...     schema=["text"]
            ... )
            >>> result_df = df.ai.extract(
            ...     input_column=col("text"),
            ...     response_format=[["name", "What is the first name?"], ["city", "What city do they work in?"]],
            ...     output_column="info",
            ... )
            >>> result_df.show()
            ------------------------------------------------------------
            |"TEXT"                          |"INFO"                   |
            ------------------------------------------------------------
            |Alice Johnson works in Seattle  |{                        |
            |                                |  "error": null,         |
            |                                |  "response": {          |
            |                                |    "city": "Seattle",   |
            |                                |    "name": "Alice"      |
            |                                |  }                      |
            |                                |}                        |
            |Bob Williams works in Portland  |{                        |
            |                                |  "error": null,         |
            |                                |  "response": {          |
            |                                |    "city": "Portland",  |
            |                                |    "name": "Bob"        |
            |                                |  }                      |
            |                                |}                        |
            ------------------------------------------------------------
            <BLANKLINE>

            >>> # Extract lists using List: prefix
            >>> df = session.create_dataframe(
            ...     [["Python, Java, and JavaScript are popular programming languages"]],
            ...     schema=["text"]
            ... )
            >>> result_df = df.ai.extract(
            ...     input_column="text",
            ...     response_format=[["languages", "List: What programming languages are mentioned?"]],
            ...     output_column="extracted",
            ... )
            >>> result_df.select("EXTRACTED").show()
            ----------------------
            |"EXTRACTED"         |
            ----------------------
            |{                   |
            |  "error": null,    |
            |  "response": {     |
            |    "languages": [  |
            |      "Python",     |
            |      "Java",       |
            |      "JavaScript"  |
            |    ]               |
            |  }                 |
            |}                   |
            ----------------------
            <BLANKLINE>

            >>> # Extract from file
            >>> from snowflake.snowpark.functions import to_file
            >>> _ = session.sql("CREATE OR REPLACE TEMP STAGE mystage ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')").collect()
            >>> _ = session.file.put("tests/resources/invoice.pdf", "@mystage", auto_compress=False)
            >>> df = session.create_dataframe([["@mystage/invoice.pdf"]], schema=["file_path"])
            >>> result_df = df.ai.extract(
            ...     input_column=to_file(col("file_path")),
            ...     response_format=[["date", "What is the invoice date?"], ["amount", "What is the amount?"]],
            ...     output_column="info",
            ... )
            >>> result_df.select("INFO").show()
            --------------------------------
            |"INFO"                        |
            --------------------------------
            |{                             |
            |  "error": null,              |
            |  "response": {               |
            |    "amount": "USD $950.00",  |
            |    "date": "Nov 26, 2016"    |
            |  }                           |
            |}                             |
            --------------------------------
            <BLANKLINE>

        """

        output_column_name = output_column or "AI_EXTRACT_OUTPUT"

        stmt = None
        input_col = _to_col_if_str(input_column, "DataFrame.ai.extract")
        if _emit_ast:
            stmt = self._dataframe._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_ai_extract, stmt)
            self._dataframe._set_ast_ref(ast.df)
            build_expr_from_snowpark_column_or_col_name(ast.input_column, input_col)
            if response_format is not None:
                build_expr_from_python_val(ast.response_format, response_format)

            ast.output_column.value = output_column_name

        result_col = ai_extract(
            input=input_col,
            response_format=response_format,
            _emit_ast=False,
        )

        output_column_name = output_column or "AI_EXTRACT_OUTPUT"
        df = self._dataframe.with_column(
            output_column_name, result_col, _emit_ast=False
        )

        add_api_call(
            df,
            "DataFrame.ai.extract",
        )
        if _emit_ast:
            df._ast_id = stmt.uid
        return df

    @experimental(version="1.39.0")
    @publicapi
    def count_tokens(
        self,
        model: str,
        prompt: ColumnOrName,
        *,
        output_column: Optional[str] = None,
        _emit_ast: bool = True,
    ) -> "snowflake.snowpark.DataFrame":
        """Count the number of tokens in text for a specified language model.

        This method returns the number of tokens that would be consumed by the specified
        model when processing the input text. This is useful for estimating costs and
        ensuring inputs fit within model token limits.

        Args:
            model: The model to base the token count on. Required. Supported models include:

                - ``deepseek-r1``, ``e5-base-v2``, ``e5-large-v2``
                - ``gemma-7b``, ``jamba-1.5-large``, ``jamba-1.5-mini``, ``jamba-instruct``
                - ``llama2-70b-chat``, ``llama3-70b``, ``llama3-8b``
                - ``llama3.1-405b``, ``llama3.1-70b``, ``llama3.1-8b``
                - ``llama3.2-1b``, ``llama3.2-3b``, ``llama3.3-70b``
                - ``llama4-maverick``, ``llama4-scout``
                - ``mistral-7b``, ``mistral-large``, ``mistral-large2``, ``mixtral-8x7b``
                - ``nv-embed-qa-4``, ``reka-core``, ``reka-flash``
                - ``snowflake-arctic-embed-l-v2.0``, ``snowflake-arctic-embed-m-v1.5``
                - ``snowflake-arctic-embed-m``, ``snowflake-arctic``
                - ``snowflake-llama-3.1-405b``, ``snowflake-llama-3.3-70b``
                - ``voyage-multilingual-2``
            prompt: The column (Column object or column name as string) containing the text
                to count tokens for.
            output_column: The name of the output column to be appended.
                If not provided, a column named ``COUNT_TOKENS_OUTPUT`` is appended.

        Returns:
            A new DataFrame with an appended output column containing the token count as an integer.

        Examples::

            >>> # Count tokens for a simple text
            >>> df = session.create_dataframe([
            ...     ["What is a large language model?"],
            ...     ["Explain quantum computing in simple terms."],
            ... ], schema=["text"])
            >>> result_df = df.ai.count_tokens(
            ...     model="llama3.1-70b",
            ...     prompt="text",
            ...     output_column="token_count"
            ... )
            >>> result_df.show()
            --------------------------------------------------------------
            |"TEXT"                                      |"TOKEN_COUNT"  |
            --------------------------------------------------------------
            |What is a large language model?             |8              |
            |Explain quantum computing in simple terms.  |9              |
            --------------------------------------------------------------
            <BLANKLINE>

        Note:
            The token count does not account for any managed system prompt that may be
            automatically added when using other Cortex AI functions. The actual token
            usage may be higher when using those functions.
        """

        output_column_name = output_column or "COUNT_TOKENS_OUTPUT"

        # AST at top
        stmt = None
        prompt_col = _to_col_if_str(prompt, "DataFrame.ai.count_tokens")
        if _emit_ast:
            stmt = self._dataframe._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_ai_count_tokens, stmt)
            self._dataframe._set_ast_ref(ast.df)
            ast.model = model
            build_expr_from_snowpark_column_or_col_name(ast.prompt, prompt_col)

            ast.output_column.value = output_column_name

        # Call SNOWFLAKE.CORTEX.COUNT_TOKENS function
        count_tokens_func = function("SNOWFLAKE.CORTEX.COUNT_TOKENS", _emit_ast=False)
        result_col = count_tokens_func(model, prompt_col)

        # Add the output column to the DataFrame
        df = self._dataframe.with_column(
            output_column_name, result_col, _emit_ast=False
        )

        add_api_call(
            df,
            "DataFrame.ai.count_tokens",
        )
        if _emit_ast:
            df._ast_id = stmt.uid
        return df

    @experimental(version="1.39.0")
    @publicapi
    def split_text_markdown_header(
        self,
        text_to_split: ColumnOrName,
        headers_to_split_on: Union[Dict[str, str], Column],
        chunk_size: Union[int, Column],
        *,
        overlap: Union[int, Column] = 0,
        output_column: Optional[str] = None,
        _emit_ast: bool = True,
    ) -> "snowflake.snowpark.DataFrame":
        """Split Markdown-formatted text into structured chunks based on header levels.

        This method segments text using specified Markdown headers and recursively splits
        each segment to produce chunks of the desired size. It preserves document structure
        by tracking which headers each chunk falls under.

        Args:
            text_to_split: The column (Column object or column name as string) containing
                the Markdown-formatted text to split.
            headers_to_split_on: A dictionary mapping Markdown header syntax to metadata field names,
                or a Column containing such a mapping. For example:
                ``{"#": "header_1", "##": "header_2"}`` will split on # and ## headers.
            chunk_size: The maximum number of characters in each chunk. Must be greater than zero.
                Can be an integer or a Column containing integer values.
            overlap: Optional number of characters to overlap between consecutive chunks.
                Defaults to 0 if not provided. Can be an integer or a Column.
            output_column: The name of the output column to be appended.
                If not provided, a column named ``SPLIT_TEXT_MARKDOWN_HEADER_OUTPUT`` is appended.

        Returns:
            A new DataFrame with an appended output column containing an array of objects.
            Each object has:

                - ``chunk``: A string containing the extracted text
                - ``headers``: A dictionary containing the Markdown header values under which the chunk is nested

        Examples::

            >>> # Split a simple Markdown document
            >>> df = session.create_dataframe([
            ...     ["# Introduction\\nThis is the intro.\\n## Background\\nSome background info."],
            ... ], schema=["document"])
            >>> result_df = df.ai.split_text_markdown_header(
            ...     text_to_split="document",
            ...     headers_to_split_on={"#": "section", "##": "subsection"},
            ...     chunk_size=20,
            ...     overlap=5,
            ...     output_column="chunks"
            ... )
            >>> result_df.show()
            --------------------------------------------------------------
            |"DOCUMENT"             |"CHUNKS"                            |
            --------------------------------------------------------------
            |# Introduction         |[                                   |
            |This is the intro.     |  {                                 |
            |## Background          |    "chunk": "This is the intro.",  |
            |Some background info.  |    "headers": {                    |
            |                       |      "section": "Introduction"     |
            |                       |    }                               |
            |                       |  },                                |
            |                       |  {                                 |
            |                       |    "chunk": "Some background",     |
            |                       |    "headers": {                    |
            |                       |      "section": "Introduction",    |
            |                       |      "subsection": "Background"    |
            |                       |    }                               |
            |                       |  },                                |
            |                       |  {                                 |
            |                       |    "chunk": "info.",               |
            |                       |    "headers": {                    |
            |                       |      "section": "Introduction",    |
            |                       |      "subsection": "Background"    |
            |                       |    }                               |
            |                       |  }                                 |
            |                       |]                                   |
            --------------------------------------------------------------
            <BLANKLINE>

        Note:
            - The function preserves document hierarchy by including parent headers for each chunk
            - Chunks are created using recursive character splitting after initial header segmentation
            - Overlap helps maintain context across chunk boundaries
        """
        method_name = "DataFrame.ai.split_text_markdown_header"
        output_column_name = output_column or "SPLIT_TEXT_MARKDOWN_HEADER_OUTPUT"

        # Convert inputs to Column objects and AST at top
        stmt = None
        text_col = _to_col_if_str(text_to_split, method_name)
        headers_col = _to_col_if_lit(headers_to_split_on, method_name)
        chunk_size_col = _to_col_if_lit(chunk_size, method_name)
        overlap_col = _to_col_if_lit(overlap, method_name)
        if _emit_ast:
            stmt = self._dataframe._session._ast_batch.bind()
            ast = with_src_position(
                stmt.expr.dataframe_ai_split_text_markdown_header, stmt
            )
            self._dataframe._set_ast_ref(ast.df)
            build_expr_from_snowpark_column_or_col_name(
                ast.text_to_split, text_to_split
            )
            build_expr_from_snowpark_column_or_python_val(
                ast.headers_to_split_on, headers_to_split_on
            )
            build_expr_from_snowpark_column_or_python_val(ast.chunk_size, chunk_size)
            build_expr_from_snowpark_column_or_python_val(ast.overlap, overlap)

            ast.output_column.value = output_column_name

        # Call SNOWFLAKE.CORTEX.SPLIT_TEXT_MARKDOWN_HEADER function
        split_func = function(
            "SNOWFLAKE.CORTEX.SPLIT_TEXT_MARKDOWN_HEADER", _emit_ast=False
        )
        result_col = split_func(text_col, headers_col, chunk_size_col, overlap_col)

        # Add the output column to the DataFrame
        df = self._dataframe.with_column(
            output_column_name, result_col, _emit_ast=False
        )

        add_api_call(
            df,
            method_name,
        )
        if _emit_ast:
            df._ast_id = stmt.uid
        return df

    @experimental(version="1.39.0")
    @publicapi
    def split_text_recursive_character(
        self,
        text_to_split: ColumnOrName,
        format: Literal["none", "markdown"],
        chunk_size: Union[int, Column],
        *,
        overlap: Union[int, Column] = 0,
        separators: Union[List[str], Column] = ("\n\n", "\n", " ", ""),
        output_column: Optional[str] = None,
        _emit_ast: bool = True,
    ) -> "snowflake.snowpark.DataFrame":
        """Split text into chunks using recursive character-based splitting.

        This method splits text by recursively trying a list of separators in order,
        creating chunks that fit within the specified size limit. It's useful for
        breaking down large documents for embedding, RAG, or search indexing.

        Args:
            text_to_split: The column (Column object or column name as string) containing
                the text to split.
            format: The format of your input text, which determines the default separators in the splitting algorithm. Must be one of the following:

                - ``none``: No format-specific separators. Only the separators in the separators field are used for splitting.
                - ``markdown``: Separates on headers, code blocks, and tables, in addition to any separators in the separators field.

            chunk_size: The maximum number of characters in each chunk. Must be greater than zero.
                Can be an integer or a Column containing integer values.
            overlap: Optional number of characters to overlap between consecutive chunks.
                Defaults to 0 if not provided. Can be an integer or a Column.
            separators: A list of separator strings to use for splitting, or a Column
                containing an array of separators. The function tries separators in order
                until it finds one that produces appropriately sized chunks.
                Defaults to ``["\\n\\n", "\\n", " ", ""]``.
            output_column: The name of the output column to be appended.
                If not provided, a column named ``SPLIT_TEXT_RECURSIVE_CHARACTER_OUTPUT`` is appended.

        Returns:
            A new DataFrame with an appended output column containing an array of text chunks.

        Examples::

            >>> # Basic text splitting without format
            >>> df = session.create_dataframe([
            ...     ["This is a long document. It has multiple sentences.\\n\\nAnd multiple paragraphs."],
            ... ], schema=["text"])
            >>> result_df = df.ai.split_text_recursive_character(
            ...     text_to_split="text",
            ...     format="none",
            ...     chunk_size=30,
            ...     overlap=5,
            ...     output_column="chunks"
            ... )
            >>> result_df.show()
            -----------------------------------------------------------------------------------------
            |"TEXT"                                              |"CHUNKS"                          |
            -----------------------------------------------------------------------------------------
            |This is a long document. It has multiple senten...  |[                                 |
            |                                                    |  "This is a long document. It",  |
            |And multiple paragraphs.                            |  "It has multiple sentences.",   |
            |                                                    |  "And multiple paragraphs."      |
            |                                                    |]                                 |
            -----------------------------------------------------------------------------------------
            <BLANKLINE>

            >>> # Split markdown formatted text
            >>> from snowflake.snowpark.functions import col
            >>> markdown_text = "# Title\\n\\n## Subtitle\\n\\nMore content."
            >>> df = session.create_dataframe([
            ...     [markdown_text],
            ... ], schema=["text"])
            >>> result_df = df.ai.split_text_recursive_character(
            ...     text_to_split=col("text"),
            ...     format="markdown",
            ...     chunk_size=25,
            ...     overlap=3,
            ...     output_column="md_chunks"
            ... )
            >>> result_df.show()
            -------------------------------------
            |"TEXT"         |"MD_CHUNKS"        |
            -------------------------------------
            |# Title        |[                  |
            |               |  "# Title",       |
            |## Subtitle    |  "## Subtitle",   |
            |               |  "More content."  |
            |More content.  |]                  |
            -------------------------------------
            <BLANKLINE>

            >>> # Custom separators with code
            >>> df = session.create_dataframe([
            ...     ["def hello():\\n    print('Hello')\\n\\ndef world():\\n    print('World')"],
            ... ], schema=["code"])
            >>> result_df = df.ai.split_text_recursive_character(
            ...     text_to_split="code",
            ...     format="none",
            ...     chunk_size=30,
            ...     separators=["\\n\\n", "\\n", "    ", " ", ""],
            ...     output_column="code_chunks"
            ... )
            >>> result_df.show()
            --------------------------------------------
            |"CODE"              |"CODE_CHUNKS"        |
            --------------------------------------------
            |def hello():        |[                    |
            |    print('Hello')  |  "def hello():",    |
            |                    |  "print('Hello')",  |
            |def world():        |  "def world():",    |
            |    print('World')  |  "print('World')"   |
            |                    |]                    |
            --------------------------------------------
            <BLANKLINE>

            >>> # Custom separators
            >>> df = session.create_dataframe([
            ...     ["First sentence. Second sentence. Third sentence.", "none", 15, 3],
            ... ], schema=["text", "fmt", "max_size", "overlap_size"])
            >>> result_df = df.ai.split_text_recursive_character(
            ...     text_to_split=col("text"),
            ...     format=col("fmt"),
            ...     chunk_size=col("max_size"),
            ...     overlap=col("overlap_size"),
            ...     separators=[". ", " ", ""],
            ...     output_column="split_text"
            ... )
            >>> result_df.select("text", "split_text").show()
            --------------------------------------------------------------------------
            |"TEXT"                                            |"SPLIT_TEXT"         |
            --------------------------------------------------------------------------
            |First sentence. Second sentence. Third sentence.  |[                    |
            |                                                  |  "First sentence",  |
            |                                                  |  ". Second",        |
            |                                                  |  "sentence",        |
            |                                                  |  ". Third",         |
            |                                                  |  "sentence."        |
            |                                                  |]                    |
            --------------------------------------------------------------------------
            <BLANKLINE>

        Note:
            - The function tries separators in the order provided
            - If no separator produces small enough chunks, it splits by individual characters
            - Overlap helps maintain context between chunks, useful for embedding and retrieval
            - Choose separators appropriate for your content type (e.g., paragraphs for prose,
              functions for code)
        """
        method_name = "DataFrame.ai.split_text_recursive_character"
        output_column_name = output_column or "SPLIT_TEXT_RECURSIVE_CHARACTER_OUTPUT"

        # Convert input to Column object & AST at top
        stmt = None
        text_col = _to_col_if_str(text_to_split, method_name)
        chunk_size_col = _to_col_if_lit(chunk_size, method_name)
        separators_col = _to_col_if_lit(separators, method_name)
        overlap_col = _to_col_if_lit(overlap, method_name)
        if _emit_ast:
            stmt = self._dataframe._session._ast_batch.bind()
            ast = with_src_position(
                stmt.expr.dataframe_ai_split_text_recursive_character, stmt
            )
            self._dataframe._set_ast_ref(ast.df)
            build_expr_from_snowpark_column_or_col_name(
                ast.text_to_split, text_to_split
            )
            if format == "markdown":
                ast.format.ai_split_text_recursive_format_markdown = True
            else:
                ast.format.ai_split_text_recursive_format_none = True
            build_expr_from_snowpark_column_or_python_val(ast.chunk_size, chunk_size)
            build_expr_from_snowpark_column_or_python_val(ast.overlap, overlap)
            build_expr_from_snowpark_column_or_python_val(ast.separators, separators)

            ast.output_column.value = output_column_name

        # Call the function
        split_func = function(
            "SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER", _emit_ast=False
        )
        result_col = split_func(
            text_col, format, chunk_size_col, overlap_col, separators_col
        )

        # Add the output column to the DataFrame
        df = self._dataframe.with_column(
            output_column_name, result_col, _emit_ast=False
        )

        add_api_call(
            df,
            method_name,
        )
        if _emit_ast:
            df._ast_id = stmt.uid
        return df
