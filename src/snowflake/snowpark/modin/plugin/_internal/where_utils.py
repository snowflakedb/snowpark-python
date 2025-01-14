#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.types import BooleanType


def validate_expected_boolean_data_columns(frame: InternalFrame) -> None:
    """
    Checks if the data column types of the frame are all boolean types.  If not, will raise an exception.

    Args:
        frame: The internal frame

    Returns:
        None
    """
    if not all(
        isinstance(
            t,
            BooleanType,
        )
        for t in frame.get_snowflake_type(
            frame.data_column_snowflake_quoted_identifiers
        )
    ):
        raise ValueError("Boolean array expected for the condition, not object")
