#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from snowflake.snowpark._internal.utils import (
    create_prompt_column_from_template,
    experimental,
)
from snowflake.snowpark.column import Column
from snowflake.snowpark.functions import ai_complete, ai_filter
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
